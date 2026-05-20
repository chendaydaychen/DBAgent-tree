#pragma once 

#include <vector>

#include "../DataFormat/row.h"

namespace storage
{
	// TODO For simplicity, the txn hisotry for OCC is oganized as follows:
	// 1. history is never deleted.
	// 2. hisotry forms a single directional list.
	//		history head -> hist_1 -> hist_2 -> hist_3 -> ... -> hist_n
	//    The head is always the latest and the tail the youngest.
	// 	  When history is traversed, always go from head -> tail order.

	class table_t;
	class Catalog;
	class txn_man;
	struct TsReqEntry;

	class set_ent{
	public:
		set_ent();
		UInt64 tn;
		txn_man * txn;
		UInt32 set_size;
		row_t ** rows;
		set_ent * next;
	};

	class Row_occ {
	public:
		void 				init(row_t * row);
		RC 					access(txn_man * txn, TsType type);
		void				read_committed(row_t * data, uint64_t * wts_out = nullptr);
		void 				latch();
		uint64_t			current_wts() const;
		// ts is the start_ts of the validating txn
		bool				validate(uint64_t ts);
		void				write(row_t * data, uint64_t ts);
		void 				release();
	private:
		pthread_mutex_t * 	_latch;
		bool 				blatch;

		row_t * 			_row;
		// the last update time
		ts_t 				wts;
	};

	class OptCC {
	public:
		void init();
		RC validate(txn_man * txn);
		void sort_accesses_for_validation(std::vector<row_t *> &rows);
		RC validate_rows_with_latch(const std::vector<row_t *> &rows, uint64_t start_ts, std::vector<row_t *> &latched_rows);
		void release_latched_rows(std::vector<row_t *> &latched_rows);
		volatile bool lock_all;
		uint64_t lock_txn_id;
	private:

		bool test_valid(set_ent * set1, set_ent * set2);
		RC get_rw_set(txn_man * txni, set_ent * &rset, set_ent *& wset);

		// "history" stores write set of transactions with tn >= smallest running tn
		set_ent * history;
		set_ent * active;
		uint64_t his_len;
		uint64_t active_len;
		volatile uint64_t tnc; // transaction number counter
		pthread_mutex_t latch;
	};
}
