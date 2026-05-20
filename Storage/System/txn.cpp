//
// Created by zwx on 3/10/26.
//

#include "txn.h"
#include "tree_txn.h"

#include <algorithm>
#include <cstring>
#include <cassert>
#include <set>

#include "DataFormat/row.h"
#include "thread.h"
#include "manager.h"
#include "mem_alloc.h"
#include "concurrency_control/occ.h"
#include "DataFormat/catalog.h"
#include "DataFormat/index_hash.h"

namespace storage
{

namespace {

struct PendingInsert {
	row_t * row {nullptr};
	itemid_t * item {nullptr};
	const StagedWrite * write {nullptr};
};

INDEX * get_data_index(txn_man * txn) {
	return txn->h_wl->indexes["Data_INDEX"];
}

table_t * get_data_table(txn_man * txn) {
	return txn->h_wl->tables["Data_TABLE"];
}

bool branch_is_writable(const BranchState * branch) {
	return branch != nullptr
		&& (branch->status == TreeBranchStatus::ACTIVE || branch->status == TreeBranchStatus::WINNER);
}

TreeReadKind default_tree_read_kind() {
	return TreeReadKind::EXPLORATORY_READ;
}

void free_snapshot_row(row_t * row) {
	if (row == nullptr) {
		return;
	}
	row->free_row();
	mem_allocator.free(row, sizeof(row_t));
}

void set_tree_conflict(TreeTxnState * state, const std::string & reason, const std::string & key = "") {
	if (state == nullptr) {
		return;
	}
	state->last_conflict.reason = reason;
	state->last_conflict.key = key;
	state->last_conflict.branch_id = state->winner_branch_id;
}

std::string format_tree_conflict(const TreeConflictInfo & conflict) {
	if (conflict.reason.empty()) {
		return "";
	}
	std::string out = conflict.reason;
	if (!conflict.key.empty()) {
		out += " key=" + conflict.key;
	}
	if (conflict.branch_id != UINT32_MAX) {
		out += " branch=" + std::to_string(conflict.branch_id);
	}
	return out;
}

row_t * find_visible_read_row(const TreeTxnState * state, uint32_t branch_id, uint64_t key_hash, const std::string & key) {
	const BranchState * branch = get_branch_state(state, branch_id);
	while (branch != nullptr) {
		for (const auto & dep : branch->normal_reads) {
			if (dep.orig_row == nullptr || dep.primary_key != key_hash) {
				continue;
			}
			char * existing_key = dep.orig_row->get_value(0);
			if (existing_key != nullptr && key == existing_key) {
				return dep.orig_row;
			}
		}
		if (branch->branch_id == branch->parent_branch_id) {
			break;
		}
		branch = get_branch_state(state, branch->parent_branch_id);
	}
	return nullptr;
}

const NormalReadDep * find_visible_read_dep(const TreeTxnState * state, uint32_t branch_id, uint64_t key_hash, const std::string & key) {
	const BranchState * branch = get_branch_state(state, branch_id);
	while (branch != nullptr) {
		for (const auto & dep : branch->normal_reads) {
			if (dep.orig_row == nullptr || dep.primary_key != key_hash) {
				continue;
			}
			char * existing_key = dep.orig_row->get_value(0);
			if (existing_key != nullptr && key == existing_key) {
				return &dep;
			}
		}
		if (branch->branch_id == branch->parent_branch_id) {
			break;
		}
		branch = get_branch_state(state, branch->parent_branch_id);
	}
	return nullptr;
}

const NormalReadDep * find_latest_visible_read_dep(const TreeTxnState * state, uint32_t branch_id, uint64_t key_hash, const std::string & key) {
	const NormalReadDep * latest = nullptr;
	const BranchState * branch = get_branch_state(state, branch_id);
	while (branch != nullptr) {
		for (const auto & dep : branch->normal_reads) {
			if (dep.orig_row == nullptr || dep.primary_key != key_hash) {
				continue;
			}
			char * existing_key = dep.orig_row->get_value(0);
			if (existing_key == nullptr || key != existing_key) {
				continue;
			}
			if (latest == nullptr
				|| dep.read_wts > latest->read_wts
				|| (dep.read_wts == latest->read_wts
					&& dep.read_kind == TreeReadKind::STRICT_READ
					&& latest->read_kind != TreeReadKind::STRICT_READ)) {
				latest = &dep;
			}
		}
		if (branch->branch_id == branch->parent_branch_id) {
			break;
		}
		branch = get_branch_state(state, branch->parent_branch_id);
	}
	return latest;
}

void add_normal_dep(BranchState & branch, row_t * row, uint64_t read_wts, TreeReadKind read_kind) {
	for (auto & dep : branch.normal_reads) {
		if (dep.orig_row == row) {
			dep.read_wts = read_wts;
			if (read_kind == TreeReadKind::STRICT_READ) {
				dep.read_kind = TreeReadKind::STRICT_READ;
			}
			return;
		}
	}
	NormalReadDep dep;
	dep.orig_row = row;
	dep.table_name = row->get_table_name();
	dep.primary_key = row->get_primary_key();
	dep.read_wts = read_wts;
	dep.read_kind = read_kind;
	branch.normal_reads.push_back(dep);
}

void add_boundary_dep(BranchState & branch, uint64_t key_hash, const std::string & key) {
	for (const auto & dep : branch.boundary_reads) {
		if (dep.key_hash == key_hash && dep.key_str == key) {
			return;
		}
	}
	BoundaryReadDep dep;
	dep.key_hash = key_hash;
	dep.key_str = key;
	dep.expected_absent = true;
	branch.boundary_reads.push_back(dep);
}

void remove_boundary_dep(BranchState & branch, uint64_t key_hash, const std::string & key) {
	branch.boundary_reads.erase(
		std::remove_if(
			branch.boundary_reads.begin(),
			branch.boundary_reads.end(),
			[&](const BoundaryReadDep & dep) {
				return dep.key_hash == key_hash && dep.key_str == key;
			}),
		branch.boundary_reads.end());
}

RC load_committed_row(txn_man * txn, const std::string & key, row_t *& row_out, std::string * value_out, uint64_t * read_wts_out = nullptr) {
	row_out = nullptr;
	INDEX * the_index = get_data_index(txn);
	if (the_index == nullptr) {
		return ERROR;
	}

	itemid_t * item = txn->index_read(the_index, txn_man::hash_key(key), 0);
	if (item == nullptr) {
		return ERROR;
	}

	row_t * row = static_cast<row_t *>(item->location);
	if (row == nullptr) {
		return Abort;
	}

	row_t * snapshot = (row_t *) mem_allocator.alloc(sizeof(row_t), row->get_part_id());
	snapshot->init(row->get_table(), row->get_part_id(), row->get_row_id());
	row->manager->read_committed(snapshot, read_wts_out);

	char * read_key = snapshot->get_value(0);
	if (!read_key || strcmp(read_key, key.c_str()) != 0) {
		free_snapshot_row(snapshot);
		return Abort;
	}

	if (value_out != nullptr) {
		char * read_val = snapshot->get_value(1);
		*value_out = read_val ? std::string(read_val) : "";
	}
	free_snapshot_row(snapshot);
	row_out = row;
	return RCOK;
}

RC load_tree_committed_row_cached(txn_man * txn, TreeTxnState * state, const std::string & key,
	row_t *& row_out, std::string * value_out, uint64_t * read_wts_out = nullptr, bool force_refresh = false) {
	row_out = nullptr;
	if (state == nullptr) {
		return ERROR;
	}

	auto cached = state->key_lookup_cache.find(key);
	if (cached != state->key_lookup_cache.end() && !force_refresh) {
		TreeKeyLookupCacheEntry & entry = cached->second;
		if (!entry.found) {
			if (value_out != nullptr) {
				value_out->clear();
			}
			if (read_wts_out != nullptr) {
				*read_wts_out = 0;
			}
			return ERROR;
		}
		if (value_out != nullptr && !entry.has_value) {
			std::string committed_value;
			uint64_t read_wts = 0;
			row_t * row = nullptr;
			RC rc = load_committed_row(txn, key, row, &committed_value, &read_wts);
			if (rc != RCOK) {
				state->key_lookup_cache.erase(cached);
				return rc;
			}
			entry.row = row;
			entry.read_wts = read_wts;
			entry.value = committed_value;
			entry.has_value = true;
		}
		row_out = entry.row;
		if (value_out != nullptr) {
			*value_out = entry.value;
		}
		if (read_wts_out != nullptr) {
			*read_wts_out = entry.read_wts;
		}
		return RCOK;
	}

	std::string committed_value;
	std::string * value_target = value_out == nullptr ? nullptr : &committed_value;
	uint64_t read_wts = 0;
	RC rc = load_committed_row(txn, key, row_out, value_target, &read_wts);
	if (rc == RCOK) {
		TreeKeyLookupCacheEntry entry;
		entry.found = true;
		entry.row = row_out;
		entry.read_wts = read_wts;
		entry.key_hash = txn_man::hash_key(key);
		if (value_out != nullptr) {
			entry.value = committed_value;
			entry.has_value = true;
			*value_out = committed_value;
		}
		if (read_wts_out != nullptr) {
			*read_wts_out = read_wts;
		}
		state->key_lookup_cache[key] = entry;
		return RCOK;
	}
	if (rc == ERROR) {
		TreeKeyLookupCacheEntry entry;
		entry.found = false;
		entry.key_hash = txn_man::hash_key(key);
		state->key_lookup_cache[key] = entry;
		if (value_out != nullptr) {
			value_out->clear();
		}
		if (read_wts_out != nullptr) {
			*read_wts_out = 0;
		}
	}
	return rc;
}

row_t * make_row_copy(row_t * orig_row, const std::string & key, const std::string & value) {
	if (orig_row == nullptr) {
		return nullptr;
	}
	row_t * temp_row = (row_t *) mem_allocator.alloc(sizeof(row_t), orig_row->get_part_id());
	temp_row->init(orig_row->get_table(), orig_row->get_part_id(), orig_row->get_row_id());
	temp_row->copy(orig_row);

	char key_buf[128] = {0};
	std::vector<char> val_buf(MAX_TUPLE_SIZE, 0);
	strncpy(key_buf, key.c_str(), sizeof(key_buf) - 1);
	strncpy(val_buf.data(), value.c_str(), MAX_TUPLE_SIZE - 1);
	temp_row->set_value(0, key_buf);
	temp_row->set_value(1, val_buf.data());
	return temp_row;
}

void free_temp_row(row_t * row) {
	if (row == nullptr) {
		return;
	}
	row->free_row();
	mem_allocator.free(row, sizeof(row_t));
}

void free_pending_insert(PendingInsert & pending) {
	if (pending.row != nullptr) {
		pending.row->free_row();
		_mm_free(pending.row);
		pending.row = nullptr;
	}
	if (pending.item != nullptr) {
		mem_allocator.free(pending.item, sizeof(itemid_t));
		pending.item = nullptr;
	}
}

} // namespace

std::hash<std::string> txn_man::hasher;

void txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	this->h_thd = h_thd;
	this->h_wl = h_wl;
	pthread_mutex_init(&txn_lock, NULL);

	row_cnt = 0;
	wr_cnt = 0;
	lock_cnt = 0;
	insert_cnt = 0;

	accesses = (Access **) _mm_malloc(sizeof(Access *) * MAX_ROW_PER_TXN, 64);
	for (int i = 0; i < MAX_ROW_PER_TXN; i++)
		accesses[i] = NULL;

	num_accesses_alloc = 0;
	record_pos.clear();
	tree_state = nullptr;
}

void txn_man::set_txn_id(txnid_t txn_id) {
	this->txn_id = txn_id;
}

txnid_t txn_man::get_txn_id() {
	return this->txn_id;
}

workload * txn_man::get_wl() {
	return h_wl;
}

uint64_t txn_man::get_thd_id() {
	return h_thd->get_thd_id();
}

void txn_man::set_ts(ts_t timestamp) {
	this->timestamp = timestamp;
}

ts_t txn_man::get_ts() {
	return this->timestamp;
}

itemid_t * txn_man::index_read(INDEX * index, idx_key_t key, int part_id) {
	itemid_t * item = nullptr;
	index->index_read(key, item, part_id, get_thd_id());
	return item;
}

void txn_man::index_read(INDEX * index, idx_key_t key, int part_id, itemid_t *& item) {
	index->index_read(key, item, part_id, get_thd_id());
}

row_t * txn_man::get_row(row_t * row, access_t type) {
	RC rc = RCOK;
	uint64_t rid = row->get_primary_key();
	auto it = record_pos.find(rid);
	if (it != record_pos.end()) {
		Access *acc = accesses[it->second];
		if (acc->type == RD && type == WR) {
			acc->type = WR;
			wr_cnt++;
		}
		return acc->data;
	}
	if (accesses[row_cnt] == NULL) {
		Access * access = (Access *) _mm_malloc(sizeof(Access), 64);
		memset(access, 0, sizeof(Access));
		accesses[row_cnt] = access;
		num_accesses_alloc++;
	}

	Access *acc = accesses[row_cnt];
	rc = row->get_row(type, this, acc->data);
	if (rc == Abort) {
		return NULL;
	}

	acc->type = type;
	acc->orig_row = row;
	record_pos[rid] = row_cnt;
	row_cnt++;
	if (type == WR) wr_cnt++;

	return acc->data;
}

void txn_man::insert_row(row_t * row, uint64_t key_hash, const std::string &key) {
	assert(insert_cnt < MAX_ROW_PER_TXN);
	insert_rows[insert_cnt].row = row;
	insert_rows[insert_cnt].key_hash = key_hash;
	insert_rows[insert_cnt].key_str = key;
	insert_cnt++;
}

RC txn_man::Read(const std::string &key, std::string &value_out, table_t * table) {
	//index search
	uint64_t primary_key = hash_key(key);
	INDEX* the_index = h_wl->indexes["Data_INDEX"];
	itemid_t *item = index_read(the_index, primary_key, 0);

	if (item == NULL) {
		return Abort;
	}

	//get row pointer and row data
	row_t *row = (row_t *)item->location;
	row_t *row_local = get_row(row, RD);
	if (!row_local) return Abort;

	//check key is correct or not
	char *read_key = row_local->get_value(0);
	if (!read_key || strcmp(read_key, key.c_str()) != 0) {
		return Abort;
	}

	//get row data
	char *read_val = row_local->get_value(1);
	value_out = read_val ? std::string(read_val) : "";
	return RCOK;
}

RC txn_man::Write(const std::string &key, const std::string &value, table_t * table) {
	uint64_t primary_key = hash_key(key);
	INDEX* the_index = h_wl->indexes["Data_INDEX"];
	table_t* the_table = h_wl->tables["Data_TABLE"];

	itemid_t *item = index_read(the_index, primary_key, 0);

	if (item == NULL) {
		row_t* new_row = nullptr;
		uint64_t row_id = 0;
		RC rc = the_table->get_new_row(new_row, 0, row_id);
		if (rc != RCOK || new_row == nullptr) return Abort;

		char key_buf[128] = {0};
		std::vector<char> val_buf(MAX_TUPLE_SIZE, 0);
		strncpy(key_buf, key.c_str(), sizeof(key_buf) - 1);
		strncpy(val_buf.data(), value.c_str(), MAX_TUPLE_SIZE - 1);
		new_row->set_primary_key(primary_key);
		new_row->set_value(0, key_buf);
		new_row->set_value(1, val_buf.data());

		row_t* row_local = get_row(new_row, WR);
		if (!row_local) return Abort;
		row_local->set_value(0, key_buf);
		row_local->set_value(1, val_buf.data());


		insert_row(new_row, primary_key, key);

		return RCOK;
	}

	// ---------- UPDATE ----------
	row_t* row = (row_t*)item->location;
	if (row == nullptr) return Abort;

	row_t* row_local = get_row(row, WR);
	if (!row_local) return Abort;

	char *exist_key = row_local->get_value(0);
	if (!exist_key || strcmp(exist_key, key.c_str()) != 0) return Abort;

	std::vector<char> val_buf(MAX_TUPLE_SIZE, 0);
	strncpy(val_buf.data(), value.c_str(), MAX_TUPLE_SIZE - 1);
	row_local->set_value(1, val_buf.data());

	return RCOK;
}

RC txn_man::finish(RC rc) {
	if (rc == RCOK)
		rc = occ_man.validate(this);
	else
		cleanup(rc);
	return rc;
}

void txn_man::release() {
	for (int i = 0; i < num_accesses_alloc; i++)
		mem_allocator.free(accesses[i], 0);
	mem_allocator.free(accesses, 0);
	reset_tree_state();
}

void txn_man::cleanup(RC rc) {
	for (int rid = row_cnt - 1; rid >= 0; rid--) {
		row_t * orig_r = accesses[rid]->orig_row;
		access_t type = accesses[rid]->type;
		if (type == WR && rc == Abort)
			type = XP;
		orig_r->return_row(type, this, accesses[rid]->data);
		accesses[rid]->data = NULL;
	}

	if (rc == RCOK || rc == Commit) {
		INDEX * the_index = get_data_index(this);
		assert(the_index != nullptr);
		for (UInt32 i = 0; i < insert_cnt; i++) {
			PendingTxnInsert & pending = insert_rows[i];
			if (pending.row == nullptr) {
				continue;
			}
			itemid_t* m_item = (itemid_t*) mem_allocator.alloc(sizeof(itemid_t), 0);
			assert(m_item != nullptr);
			m_item->type = DT_row;
			m_item->location = pending.row;
			m_item->next = nullptr;
			m_item->valid = true;
			RC insert_rc = the_index->index_insert(pending.key_hash, m_item, 0);
			assert(insert_rc == RCOK);
			pending.row = nullptr;
		}
	} else {
		for (UInt32 i = 0; i < insert_cnt; i++) {
			row_t * row = insert_rows[i].row;
			if (row == nullptr) {
				continue;
			}
			assert(g_part_alloc == false);
			row->free_row();
			_mm_free(row);
			insert_rows[i].row = nullptr;
		}
	}

	row_cnt = 0;
	wr_cnt = 0;
	insert_cnt = 0;
	record_pos.clear();
}

void txn_man::reset_tree_state() {
	if (tree_state != nullptr) {
		if (!tree_state->last_conflict.reason.empty()) {
			last_tree_conflict_summary = format_tree_conflict(tree_state->last_conflict);
		}
		delete tree_state;
		tree_state = nullptr;
	}
}

RC txn_man::tree_begin() {
	reset_tree_state();
	last_tree_conflict_summary.clear();
	tree_state = new TreeTxnState();
	tree_state->last_conflict.clear();
	tree_state->active = true;
	tree_state->root_branch_id = 0;
	tree_state->next_branch_id = 1;
	tree_state->winner_branch_id = UINT32_MAX;

	BranchState root;
	root.branch_id = 0;
	root.parent_branch_id = 0;
	root.status = TreeBranchStatus::ACTIVE;
	tree_state->branches.emplace(root.branch_id, root);

	start_ts = glob_manager->get_ts(get_thd_id());
	return RCOK;
}

RC txn_man::tree_create_branch(uint32_t parent_branch_id, uint32_t &branch_id_out) {
	branch_id_out = UINT32_MAX;
	if (tree_state == nullptr || !tree_state->active) {
		return ERROR;
	}
	if (tree_state->winner_branch_id != UINT32_MAX) {
		return ERROR;
	}
	if (parent_branch_id != tree_state->root_branch_id) {
		return ERROR;
	}

	BranchState * parent = get_branch_state(tree_state, parent_branch_id);
	if (!branch_is_writable(parent)) {
		return ERROR;
	}

	BranchState child;
	child.branch_id = tree_state->next_branch_id++;
	child.parent_branch_id = parent_branch_id;
	child.status = TreeBranchStatus::ACTIVE;
	tree_state->branches.emplace(child.branch_id, child);
	branch_id_out = child.branch_id;
	return RCOK;
}

RC txn_man::tree_read(uint32_t branch_id, const std::string &key, std::string &value_out, table_t * table, bool force_strict) {
	(void)table;
	value_out.clear();
	if (tree_state == nullptr || !tree_state->active) {
		return ERROR;
	}

	BranchState * branch = get_branch_state(tree_state, branch_id);
	if (!branch_is_writable(branch)) {
		return ERROR;
	}

	const bool strict_read = force_strict || tree_state->winner_branch_id != UINT32_MAX;
	const StagedWrite * visible = find_visible_write(tree_state, branch_id, key);
	if (visible != nullptr && !strict_read) {
		value_out = visible->value();
		return RCOK;
	}

	row_t * row = nullptr;
	std::string committed_value;
	uint64_t read_wts = 0;
	RC rc = load_tree_committed_row_cached(this, tree_state, key, row, &committed_value, &read_wts, strict_read);
	if (rc == RCOK) {
		add_normal_dep(*branch, row, read_wts, strict_read ? TreeReadKind::STRICT_READ : default_tree_read_kind());
		value_out = committed_value;
		return RCOK;
	}
	if (rc == ERROR) {
		add_boundary_dep(*branch, hash_key(key), key);
		value_out.clear();
		return RCOK;
	}
	return rc;
}

RC txn_man::tree_write(uint32_t branch_id, const std::string &key, const std::string &value, table_t * table) {
	(void)table;
	if (tree_state == nullptr || !tree_state->active) {
		return ERROR;
	}

	BranchState * branch = get_branch_state(tree_state, branch_id);
	if (!branch_is_writable(branch)) {
		return ERROR;
	}
	if (branch_id == tree_state->root_branch_id) {
		return ERROR;
	}

	uint64_t key_hash = hash_key(key);
	auto shared_value = std::make_shared<const std::string>(value);
	const StagedWrite * visible = find_visible_write(tree_state, branch_id, key);
	if (visible != nullptr) {
		const NormalReadDep * read_dep = find_latest_visible_read_dep(tree_state, branch_id, key_hash, key);
		StagedWrite staged = *visible;
		if (read_dep != nullptr && read_dep->orig_row != nullptr) {
			// A retryable Tree commit can discover that an insert-absent winner key
			// was created by a concurrent writer. After a strict refresh, the
			// winner write becomes an update against the refreshed committed row.
			staged.kind = StagedWriteKind::UPDATE_EXISTING;
			staged.orig_row = read_dep->orig_row;
			add_normal_dep(*branch, staged.orig_row, read_dep->read_wts, TreeReadKind::STRICT_READ);
			staged.base_wts = read_dep->read_wts;
			remove_boundary_dep(*branch, key_hash, key);
		}
		staged.share_value(shared_value);
		branch->staged_writes[key] = staged;
		return RCOK;
	}

	row_t * cached_row = find_visible_read_row(tree_state, branch_id, key_hash, key);
	if (cached_row != nullptr) {
		const NormalReadDep * read_dep = find_latest_visible_read_dep(tree_state, branch_id, key_hash, key);
		if (read_dep != nullptr) {
			add_normal_dep(*branch, cached_row, read_dep->read_wts, TreeReadKind::STRICT_READ);
		}
		StagedWrite staged;
		staged.kind = StagedWriteKind::UPDATE_EXISTING;
		staged.key_str = key;
		staged.share_value(shared_value);
		staged.key_hash = key_hash;
		staged.orig_row = cached_row;
		staged.base_wts = read_dep ? read_dep->read_wts : 0;
		branch->staged_writes[key] = staged;
		return RCOK;
	}

	row_t * row = nullptr;
	uint64_t read_wts = 0;
	RC rc = load_tree_committed_row_cached(this, tree_state, key, row, nullptr, &read_wts);
	if (rc == RCOK) {
		add_normal_dep(*branch, row, read_wts, TreeReadKind::STRICT_READ);
		StagedWrite staged;
		staged.kind = StagedWriteKind::UPDATE_EXISTING;
		staged.key_str = key;
		staged.share_value(shared_value);
		staged.key_hash = key_hash;
		staged.orig_row = row;
		staged.base_wts = read_wts;
		branch->staged_writes[key] = staged;
		return RCOK;
	}
	if (rc == ERROR) {
		add_boundary_dep(*branch, key_hash, key);
		StagedWrite staged;
		staged.kind = StagedWriteKind::INSERT_ABSENT;
		staged.key_str = key;
		staged.share_value(shared_value);
		staged.key_hash = key_hash;
		staged.orig_row = nullptr;
		branch->staged_writes[key] = staged;
		return RCOK;
	}
	return rc;
}

RC txn_man::tree_select_winner(uint32_t branch_id) {
	if (tree_state == nullptr || !tree_state->active) {
		return ERROR;
	}
	BranchState * branch = get_branch_state(tree_state, branch_id);
	if (!branch_is_writable(branch)) {
		return ERROR;
	}

	if (tree_state->winner_branch_id != UINT32_MAX) {
		BranchState * old_winner = get_branch_state(tree_state, tree_state->winner_branch_id);
		if (old_winner != nullptr && old_winner->status == TreeBranchStatus::WINNER) {
			old_winner->status = TreeBranchStatus::ACTIVE;
		}
	}

	tree_state->winner_branch_id = branch_id;
	branch->status = TreeBranchStatus::WINNER;
	for (auto & entry : tree_state->branches) {
		BranchState & candidate = entry.second;
		if (candidate.branch_id == tree_state->root_branch_id || candidate.branch_id == branch_id) {
			continue;
		}
		candidate.status = TreeBranchStatus::DROPPED;
		candidate.staged_writes.clear();
		candidate.normal_reads.clear();
		candidate.boundary_reads.clear();
	}
	return RCOK;
}

RC txn_man::tree_commit() {
	return tree_commit_impl(false);
}

RC txn_man::tree_commit_retryable() {
	return tree_commit_impl(true);
}

RC txn_man::tree_commit_impl(bool preserve_on_abort) {
	if (tree_state == nullptr || !tree_state->active || tree_state->winner_branch_id == UINT32_MAX) {
		return ERROR;
	}

	TreeCommitPlan plan;
	if (!build_tree_commit_plan(tree_state, plan)) {
		set_tree_conflict(tree_state, "PLAN_INVALID");
		reset_tree_state();
		return ERROR;
	}

	auto * the_index = dynamic_cast<IndexHash *>(get_data_index(this));
	table_t * the_table = get_data_table(this);
	if (the_index == nullptr || the_table == nullptr) {
		set_tree_conflict(tree_state, "MISSING_STORAGE");
		reset_tree_state();
		return ERROR;
	}

	std::vector<row_t *> validate_rows;
	std::unordered_map<row_t *, uint64_t> expected_wts;
	for (const auto & dep : plan.normal_reads) {
		if (dep.read_kind != TreeReadKind::STRICT_READ || dep.orig_row == nullptr) {
			continue;
		}
		auto it = expected_wts.find(dep.orig_row);
		if (it != expected_wts.end() && it->second != dep.read_wts) {
			set_tree_conflict(tree_state, "READ_DEP_VERSION_MISMATCH");
			if (!preserve_on_abort) {
				reset_tree_state();
			}
			return Abort;
		}
		expected_wts[dep.orig_row] = dep.read_wts;
		validate_rows.push_back(dep.orig_row);
	}
	for (const auto & write : plan.final_writes) {
		if (write.kind == StagedWriteKind::UPDATE_EXISTING && write.orig_row != nullptr) {
			auto it = expected_wts.find(write.orig_row);
			if (it != expected_wts.end() && it->second != write.base_wts) {
				set_tree_conflict(tree_state, "WRITE_BASE_VERSION_MISMATCH", write.key_str);
				if (!preserve_on_abort) {
					reset_tree_state();
				}
				return Abort;
			}
			expected_wts[write.orig_row] = write.base_wts;
			validate_rows.push_back(write.orig_row);
		}
	}

	std::vector<row_t *> latched_rows;
	occ_man.sort_accesses_for_validation(validate_rows);
	validate_rows.erase(std::unique(validate_rows.begin(), validate_rows.end()), validate_rows.end());
	RC rc = RCOK;
	for (row_t * row : validate_rows) {
		row->manager->latch();
		latched_rows.push_back(row);
		auto expected_it = expected_wts.find(row);
		if (expected_it == expected_wts.end()) {
			continue;
		}
		if (row->manager->current_wts() != expected_it->second) {
			occ_man.release_latched_rows(latched_rows);
			set_tree_conflict(tree_state, "STRICT_READ_VERSION_CHANGED");
			if (!preserve_on_abort) {
				reset_tree_state();
			}
			return Abort;
		}
	}

	std::vector<BucketHeader *> buckets_to_lock;
	std::unordered_map<uint64_t, BucketHeader *> bucket_cache;
	auto ensure_bucket = [&](uint64_t key_hash) -> BucketHeader * {
		auto cached = bucket_cache.find(key_hash);
		if (cached != bucket_cache.end()) {
			return cached->second;
		}
		BucketHeader * bucket = the_index->locate_bucket(key_hash, 0);
		if (bucket != nullptr) {
			bucket_cache.emplace(key_hash, bucket);
		}
		return bucket;
	};
	for (const auto & dep : plan.boundary_reads) {
		BucketHeader * bucket = ensure_bucket(dep.key_hash);
		if (bucket == nullptr) {
			set_tree_conflict(tree_state, "BOUNDARY_BUCKET_MISSING", dep.key_str);
			rc = ERROR;
			break;
		}
		buckets_to_lock.push_back(bucket);
	}
	for (const auto & write : plan.final_writes) {
		if (write.kind == StagedWriteKind::INSERT_ABSENT) {
			BucketHeader * bucket = ensure_bucket(write.key_hash);
			if (bucket == nullptr) {
				set_tree_conflict(tree_state, "INSERT_BUCKET_MISSING", write.key_str);
				rc = ERROR;
				break;
			}
			buckets_to_lock.push_back(bucket);
		}
	}

	std::sort(buckets_to_lock.begin(), buckets_to_lock.end());
	buckets_to_lock.erase(std::unique(buckets_to_lock.begin(), buckets_to_lock.end()), buckets_to_lock.end());
	for (BucketHeader * bucket : buckets_to_lock) {
		the_index->latch_bucket(bucket);
	}

	if (rc == RCOK) {
		for (const auto & dep : plan.boundary_reads) {
			BucketHeader * bucket = ensure_bucket(dep.key_hash);
			if (bucket == nullptr) {
				set_tree_conflict(tree_state, "BOUNDARY_BUCKET_MISSING", dep.key_str);
				rc = ERROR;
				break;
			}
			if (dep.expected_absent &&
				the_index->bucket_contains_exact_key(bucket, dep.key_hash, dep.key_str, the_table->get_table_name())) {
				set_tree_conflict(tree_state, "BOUNDARY_KEY_APPEARED", dep.key_str);
				rc = Abort;
				break;
			}
		}
	}

	if (rc == RCOK) {
		for (const auto & write : plan.final_writes) {
			if (write.kind != StagedWriteKind::INSERT_ABSENT) {
				continue;
			}
			BucketHeader * bucket = ensure_bucket(write.key_hash);
			if (bucket == nullptr) {
				set_tree_conflict(tree_state, "INSERT_BUCKET_MISSING", write.key_str);
				rc = ERROR;
				break;
			}
			if (the_index->bucket_contains_exact_key(bucket, write.key_hash, write.key_str, the_table->get_table_name())) {
				set_tree_conflict(tree_state, "INSERT_KEY_EXISTS", write.key_str);
				rc = Abort;
				break;
			}
		}
	}

	std::vector<PendingInsert> pending_inserts;
	if (rc == RCOK) {
		end_ts = glob_manager->get_ts(get_thd_id());
		for (const auto & write : plan.final_writes) {
			if (write.kind != StagedWriteKind::INSERT_ABSENT) {
				continue;
			}

			PendingInsert pending;
			pending.write = &write;

			uint64_t row_id = 0;
			rc = the_table->get_new_row(pending.row, 0, row_id);
			if (rc != RCOK || pending.row == nullptr) {
				set_tree_conflict(tree_state, "INSERT_ROW_ALLOC_FAILED", write.key_str);
				rc = Abort;
				free_pending_insert(pending);
				break;
			}

			char key_buf[128] = {0};
			std::vector<char> val_buf(MAX_TUPLE_SIZE, 0);
			strncpy(key_buf, write.key_str.c_str(), sizeof(key_buf) - 1);
			strncpy(val_buf.data(), write.value().c_str(), MAX_TUPLE_SIZE - 1);
			pending.row->set_primary_key(write.key_hash);
			pending.row->set_value(0, key_buf);
			pending.row->set_value(1, val_buf.data());
			pending.row->manager->write(pending.row, end_ts);

			pending.item = (itemid_t *) mem_allocator.alloc(sizeof(itemid_t), 0);
			if (pending.item == nullptr) {
				set_tree_conflict(tree_state, "INSERT_ITEM_ALLOC_FAILED", write.key_str);
				rc = Abort;
				free_pending_insert(pending);
				break;
			}
			pending.item->type = DT_row;
			pending.item->location = pending.row;
			pending.item->next = nullptr;
			pending.item->valid = true;
			pending_inserts.push_back(pending);
		}
	}

	if (rc == RCOK) {
		for (const auto & write : plan.final_writes) {
			if (write.kind != StagedWriteKind::UPDATE_EXISTING) {
				continue;
			}
			row_t * temp_row = make_row_copy(write.orig_row, write.key_str, write.value());
			if (temp_row == nullptr) {
				set_tree_conflict(tree_state, "UPDATE_ROW_COPY_FAILED", write.key_str);
				rc = Abort;
				break;
			}
			write.orig_row->manager->write(temp_row, end_ts);
			free_temp_row(temp_row);
		}
	}

	if (rc == RCOK) {
		for (const auto & pending : pending_inserts) {
			BucketHeader * bucket = ensure_bucket(pending.write->key_hash);
			if (bucket == nullptr) {
				set_tree_conflict(tree_state, "INSERT_BUCKET_MISSING", pending.write->key_str);
				rc = ERROR;
				break;
			}
			// The bucket is already latched in the certification phase above.
			// Re-entering index_insert() here would try to latch the same bucket
			// again and self-deadlock on insert-absent commits.
			bucket->insert_item(pending.write->key_hash, pending.item, 0);
			if (rc != RCOK) {
				break;
			}
		}
	}

	for (BucketHeader * bucket : buckets_to_lock) {
		the_index->unlatch_bucket(bucket);
	}
	occ_man.release_latched_rows(latched_rows);

	if (rc != RCOK) {
		for (auto & pending : pending_inserts) {
			free_pending_insert(pending);
		}
		if (!preserve_on_abort) {
			reset_tree_state();
		}
		return rc == ERROR ? ERROR : Abort;
	}

	for (auto & pending : pending_inserts) {
		pending.row = nullptr;
		pending.item = nullptr;
	}

	reset_tree_state();
	return RCOK;
}

std::string txn_man::tree_last_conflict_reason() const {
	if (tree_state == nullptr) {
		return last_tree_conflict_summary;
	}
	std::string current = format_tree_conflict(tree_state->last_conflict);
	return current.empty() ? last_tree_conflict_summary : current;
}

RC txn_man::tree_abort() {
	if (tree_state == nullptr || !tree_state->active) {
		return ERROR;
	}
	reset_tree_state();
	return RCOK;
}

} // namespace storage
