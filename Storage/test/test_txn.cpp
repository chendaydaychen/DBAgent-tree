#include "test.h"
#include "row.h"
#include "index_hash.h"
#include "thread.h"
#include "manager.h"

#include <string>

namespace storage
{
	namespace {

	row_t * find_exact_row(TestWorkload * wl, const std::string & key) {
		uint64_t key_hash = txn_man::hash_key(key);
		auto * index = static_cast<IndexHash *>(wl->the_index);
		BucketHeader * bucket = index->locate_bucket(key_hash, 0);
		if (bucket == nullptr) {
			return nullptr;
		}
		for (BucketNode * node = bucket->first_node; node != NULL; node = node->next) {
			if (node->key != key_hash) {
				continue;
			}
			for (itemid_t * item = node->items; item != NULL; item = item->next) {
				auto * row = static_cast<row_t *>(item->location);
				if (row == nullptr) {
					continue;
				}
				char * row_key = row->get_value(0);
				if (row_key != nullptr && key == row_key) {
					return row;
				}
			}
		}
		return nullptr;
	}

	std::string committed_value(TestWorkload * wl, const std::string & key) {
		row_t * row = find_exact_row(wl, key);
		if (row == nullptr) {
			return "";
		}
		char * value = row->get_value(1);
		return value ? std::string(value) : "";
	}

	bool committed_exists(TestWorkload * wl, const std::string & key) {
		return find_exact_row(wl, key) != nullptr;
	}

	TestTxnMan * alloc_txn(TestWorkload * wl, thread_t *& thd) {
		thd = new thread_t();
		thd->init(0, wl);
		txn_man * txn = nullptr;
		wl->get_txn_man(txn, thd);
		auto * test_txn = static_cast<TestTxnMan *>(txn);
		test_txn->start_ts = glob_manager->get_ts(test_txn->get_thd_id());
		return test_txn;
	}

	} // namespace

	void TestTxnMan::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
		txn_man::init(h_thd, h_wl, thd_id);
		_wl = (TestWorkload *) h_wl;
	}

	RC TestTxnMan::run_txn(int type, int access_num) {
		switch(type) {
		case READ_WRITE :
			return testReadwrite(access_num);
		case CONFLICT:
			return testConflict(access_num);
		case TREE_WINNER_OVERRIDE:
			return testTreeWinnerOverride();
		case TREE_SIBLING_ISOLATION:
			return testTreeSiblingIsolation();
		case TREE_EXPLORATORY_ROOT_READ:
			return testTreeExploratoryRootRead();
		case TREE_WINNER_REFRESH_UPDATE:
			return testTreeWinnerRefreshUpdate();
		case TREE_CONFLICT_ABSENT_INSERT:
			return testTreeConflictOnAbsentInsert();
		case TREE_ABORT_CLEANUP:
			return testTreeAbortCleanup();
		default:
			assert(false);
		}
	}

	RC TestTxnMan::testReadwrite(int access_num) {
		RC rc = RCOK;
		itemid_t * m_item;

		m_item = index_read(_wl->the_index, 0, 0);
		row_t * row = ((row_t *)m_item->location);
		row_t * row_local = get_row(row, WR);
		if (access_num == 0) {
			char str[] = "hello";
			row_local->set_value(0, 1234);
			row_local->set_value(1, 1234.5);
			row_local->set_value(2, 8589934592UL);
			row_local->set_value(3, str);
		} else {
			int v1;
			double v2;
			uint64_t v3;
			char * v4;

			row_local->get_value(0, v1);
			row_local->get_value(1, v2);
			row_local->get_value(2, v3);
			v4 = row_local->get_value(3);

			assert(v1 == 1234);
			assert(v2 == 1234.5);
			assert(v3 == 8589934592UL);
			assert(strcmp(v4, "hello") == 0);
		}
		rc = finish(rc);
		if (access_num == 0)
			return RCOK;
		else
			return FINISH;
	}

	RC
	TestTxnMan::testConflict(int access_num)
	{
		RC rc = RCOK;
		itemid_t * m_item;

		idx_key_t key;
		for (key = 0; key < 1; key ++) {
			m_item = index_read(_wl->the_index, key, 0);
			row_t * row = ((row_t *)m_item->location);
			row_t * row_local;
			row_local = get_row(row, WR);
			if (row_local) {
				char str[] = "hello";
				row_local->set_value(0, 1234);
				row_local->set_value(1, 1234.5);
				row_local->set_value(2, 8589934592UL);
				row_local->set_value(3, str);
				sleep(1);
			} else {
				rc = Abort;
				break;
			}
		}
		rc = finish(rc);
		return rc;
	}

	RC TestTxnMan::testTreeWinnerOverride() {
		const std::string key = "0";
		RC rc = tree_begin();
		assert(rc == RCOK);
		rc = tree_write(0, key, "tree_root");
		assert(rc == ERROR);
		uint32_t branch_id = 0;
		rc = tree_create_branch(0, branch_id);
		assert(rc == RCOK);
		rc = tree_write(branch_id, key, "tree_child");
		assert(rc == RCOK);
		rc = tree_select_winner(branch_id);
		assert(rc == RCOK);
		rc = tree_commit();
		assert(rc == RCOK);
		assert(committed_value(_wl, key) == "tree_child");
		return RCOK;
	}

	RC TestTxnMan::testTreeSiblingIsolation() {
		const std::string root_key = "2";
		const std::string sibling_key = "3";
		RC rc = tree_begin();
		assert(rc == RCOK);

		uint32_t branch_a = 0;
		uint32_t branch_b = 0;
		rc = tree_create_branch(0, branch_a);
		assert(rc == RCOK);
		rc = tree_create_branch(0, branch_b);
		assert(rc == RCOK);

		rc = tree_write(branch_a, sibling_key, "sibling_a");
		assert(rc == RCOK);

		std::string read_value;
		rc = tree_read(branch_a, root_key, read_value);
		assert(rc == RCOK);
		assert(read_value == "2");

		rc = tree_read(branch_b, sibling_key, read_value);
		assert(rc == RCOK);
		assert(read_value == "3");

		rc = tree_write(branch_b, root_key, "winner_branch_b");
		assert(rc == RCOK);
		rc = tree_select_winner(branch_b);
		assert(rc == RCOK);
		rc = tree_commit();
		assert(rc == RCOK);
		assert(committed_value(_wl, root_key) == "winner_branch_b");
		assert(committed_value(_wl, sibling_key) == "3");
		return RCOK;
	}

	RC TestTxnMan::testTreeExploratoryRootRead() {
		const std::string key = "4";
		RC rc = tree_begin();
		assert(rc == RCOK);

		std::string read_value;
		rc = tree_read(0, key, read_value);
		assert(rc == RCOK);
		assert(!read_value.empty());

		thread_t * ext_thd = nullptr;
		TestTxnMan * ext_txn = alloc_txn(_wl, ext_thd);
		rc = ext_txn->Write(key, "external_existing");
		assert(rc == RCOK);
		rc = ext_txn->finish(RCOK);
		assert(rc == RCOK);

		rc = tree_select_winner(0);
		assert(rc == RCOK);
		rc = tree_commit();
		assert(rc == RCOK);
		assert(committed_value(_wl, key) == "external_existing");
		return RCOK;
	}

	RC TestTxnMan::testTreeWinnerRefreshUpdate() {
		const std::string key = "6";
		RC rc = tree_begin();
		assert(rc == RCOK);

		uint32_t branch_id = 0;
		rc = tree_create_branch(0, branch_id);
		assert(rc == RCOK);

		std::string read_value;
		rc = tree_read(branch_id, key, read_value);
		assert(rc == RCOK);
		assert(!read_value.empty());
		rc = tree_write(branch_id, key, "branch_speculative");
		assert(rc == RCOK);

		thread_t * ext_thd = nullptr;
		TestTxnMan * ext_txn = alloc_txn(_wl, ext_thd);
		rc = ext_txn->Write(key, "external_refresh");
		assert(rc == RCOK);
		rc = ext_txn->finish(RCOK);
		assert(rc == RCOK);

		rc = tree_select_winner(branch_id);
		assert(rc == RCOK);
		rc = tree_read(0, key, read_value);
		assert(rc == RCOK);
		assert(read_value == "external_refresh");

		rc = tree_write(branch_id, key, "winner_refresh");
		assert(rc == RCOK);
		rc = tree_commit();
		assert(rc == RCOK);
		assert(committed_value(_wl, key) == "winner_refresh");
		return RCOK;
	}

	RC TestTxnMan::testTreeConflictOnAbsentInsert() {
		const std::string key = "tree_conflict_insert_v1";
		assert(!committed_exists(_wl, key));

		RC rc = tree_begin();
		assert(rc == RCOK);
		uint32_t branch_id = 0;
		rc = tree_create_branch(0, branch_id);
		assert(rc == RCOK);
		rc = tree_write(branch_id, key, "tree_pending");
		assert(rc == RCOK);

		thread_t * ext_thd = nullptr;
		TestTxnMan * ext_txn = alloc_txn(_wl, ext_thd);
		rc = ext_txn->Write(key, "external_insert");
		assert(rc == RCOK);
		rc = ext_txn->finish(RCOK);
		assert(rc == RCOK);

		rc = tree_select_winner(branch_id);
		assert(rc == RCOK);
		rc = tree_commit();
		assert(rc == Abort);
		assert(committed_value(_wl, key) == "external_insert");
		return RCOK;
	}

	RC TestTxnMan::testTreeAbortCleanup() {
		const std::string key = "tree_abort_cleanup_v1";
		assert(!committed_exists(_wl, key));

		RC rc = tree_begin();
		assert(rc == RCOK);
		uint32_t branch_id = 0;
		rc = tree_create_branch(0, branch_id);
		assert(rc == RCOK);
		rc = tree_write(branch_id, key, "temp_value");
		assert(rc == RCOK);
		rc = tree_write(branch_id, "5", "branch_write");
		assert(rc == RCOK);

		rc = tree_abort();
		assert(rc == RCOK);
		assert(!committed_exists(_wl, key));
		assert(committed_value(_wl, "5") == "5");
		return RCOK;
	}
}
