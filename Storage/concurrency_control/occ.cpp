#include "../System/global.h"
#include "../System/helper.h"
#include "../System/txn.h"
#include "../DataFormat/row.h"
#include "occ.h"
#include "../System/manager.h"
#include "../System/mem_alloc.h"

#include <algorithm>

namespace storage
{
	set_ent::set_ent() {
		set_size = 0;
		txn = NULL;
		rows = NULL;
		next = NULL;
	}

	void OptCC::init() {
		tnc = 0;
		his_len = 0;
		active_len = 0;
		active = NULL;
		history = NULL;
		lock_all = false;
		lock_txn_id = 0;
		pthread_mutex_init(&latch, NULL);
	}

	void OptCC::sort_accesses_for_validation(std::vector<row_t *> &rows) {
		std::sort(rows.begin(), rows.end(), [](row_t * lhs, row_t * rhs) {
			int tabcmp = strcmp(lhs->get_table_name(), rhs->get_table_name());
			if (tabcmp != 0) {
				return tabcmp < 0;
			}
			if (lhs->get_primary_key() != rhs->get_primary_key()) {
				return lhs->get_primary_key() < rhs->get_primary_key();
			}
			return lhs < rhs;
		});
		rows.erase(std::unique(rows.begin(), rows.end()), rows.end());
	}

	RC OptCC::validate_rows_with_latch(const std::vector<row_t *> &rows, uint64_t start_ts, std::vector<row_t *> &latched_rows) {
		latched_rows.clear();
		if (rows.empty()) {
			return RCOK;
		}

		std::vector<row_t *> ordered = rows;
		sort_accesses_for_validation(ordered);
		for (row_t * row : ordered) {
			row->manager->latch();
			latched_rows.push_back(row);
			if (!row->manager->validate(start_ts)) {
				release_latched_rows(latched_rows);
				return Abort;
			}
		}
		return RCOK;
	}

	void OptCC::release_latched_rows(std::vector<row_t *> &latched_rows) {
		for (auto it = latched_rows.rbegin(); it != latched_rows.rend(); ++it) {
			(*it)->manager->release();
		}
		latched_rows.clear();
	}

	RC OptCC::validate(txn_man * txn) {
		RC rc = RCOK;
		std::vector<row_t *> rows;
		rows.reserve(txn->row_cnt);
		for (int i = 0; i < txn->row_cnt; i++) {
			rows.push_back(txn->accesses[i]->orig_row);
		}

		std::vector<row_t *> latched_rows;
		rc = validate_rows_with_latch(rows, txn->start_ts, latched_rows);
		txn->lock_cnt = latched_rows.size();
		if (rc == RCOK) {
			// Validation passed.
			// advance the global timestamp and get the end_ts
			txn->end_ts = glob_manager->get_ts( txn->get_thd_id() );
			// write to each row and update wts
			txn->cleanup(RCOK);
		} else {
			txn->cleanup(Abort);
		}
		release_latched_rows(latched_rows);

		return rc;
	}

	RC OptCC::get_rw_set(txn_man * txn, set_ent * &rset, set_ent *& wset) {
		wset = (set_ent*) mem_allocator.alloc(sizeof(set_ent), 0);
		rset = (set_ent*) mem_allocator.alloc(sizeof(set_ent), 0);
		wset->set_size = txn->wr_cnt;
		rset->set_size = txn->row_cnt - txn->wr_cnt;
		wset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * wset->set_size, 0);
		rset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * rset->set_size, 0);
		wset->txn = txn;
		rset->txn = txn;

		UInt32 n = 0, m = 0;
		for (int i = 0; i < txn->row_cnt; i++) {
			if (txn->accesses[i]->type == WR)
				wset->rows[n ++] = txn->accesses[i]->orig_row;
			else
				rset->rows[m ++] = txn->accesses[i]->orig_row;
		}

		assert(n == wset->set_size);
		assert(m == rset->set_size);
		return RCOK;
	}

	bool OptCC::test_valid(set_ent * set1, set_ent * set2) {
		for (UInt32 i = 0; i < set1->set_size; i++)
			for (UInt32 j = 0; j < set2->set_size; j++) {
				if (set1->rows[i] == set2->rows[j]) {
					return false;
				}
			}
		return true;
	}
}
