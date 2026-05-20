//
// Created by zwx on 3/10/26.
//
#pragma once
#ifndef TXN_H
#define TXN_H

#include <string>
#include <unordered_map>

#include "global.h"
#include "helper.h"
#include "wl.h"
#include "table.h"

namespace storage
{
	class thread_t;
	class row_t;
	class base_query;
	class INDEX;
	struct TreeTxnState;


	// each thread has a txn_man.
	// a txn_man corresponds to a single transaction.

	class Access {
	public:
		access_t 	type;
		row_t * 	orig_row;
		row_t * 	data;
		row_t * 	orig_data;
		void cleanup();
	};

	enum class op_t {
		Update_OP,      // 可更新已存在key（你也可以定义为upsert）
		INSERT_OP    // 仅新key允许
	};
	struct write_t {
		op_t type;
		std::string key_str;   // 原始key字符串（用于防hash冲突复核）
		std::string val_str;
		row_t * wr_local;
	};

	struct PendingTxnInsert {
		row_t * row {nullptr};
		uint64_t key_hash {0};
		std::string key_str;
	};

	class txn_man
	{
	public:
		virtual void init(thread_t * h_thd, workload * h_wl, uint64_t part_id);
		virtual size_t alloc_size() const { return sizeof(txn_man); }
		void release();
		thread_t * h_thd;
		workload * h_wl;
		myrand * mrand;
		uint64_t abort_cnt;

		virtual RC 		run_txn(base_query * m_query) = 0;
		uint64_t 		get_thd_id();
		workload * 		get_wl();
		void 			set_txn_id(txnid_t txn_id);
		txnid_t 		get_txn_id();

		void 			set_ts(ts_t timestamp);
		ts_t 			get_ts();

		pthread_mutex_t txn_lock;
		row_t * volatile cur_row;

		RC 				finish(RC rc);
		void			applywrite();
		void 			cleanup(RC rc);
		RC				tree_begin();
		RC				tree_create_branch(uint32_t parent_branch_id, uint32_t &branch_id_out);
		RC				tree_read(uint32_t branch_id, const std::string &key, std::string &value_out, table_t * table = nullptr, bool force_strict = false);
		RC				tree_write(uint32_t branch_id, const std::string &key, const std::string &value, table_t * table = nullptr);
		RC				tree_select_winner(uint32_t branch_id);
		RC				tree_commit();
		RC				tree_commit_retryable();
		std::string		tree_last_conflict_reason() const;
		RC				tree_abort();

		// For OCC
		uint64_t 		start_ts;
		uint64_t 		end_ts;
		// following are public for OCC
		int 			row_cnt;
		int	 			wr_cnt;
		int				lock_cnt;
		Access **		accesses;
		int 			num_accesses_alloc;

		itemid_t *		index_read(INDEX * index, idx_key_t key, int part_id);
		void 			index_read(INDEX * index, idx_key_t key, int part_id, itemid_t *& item);
		row_t * 		get_row(row_t * row, access_t type);
		void 			insert_row(row_t * row, uint64_t key_hash, const std::string &key);

		static  uint64_t hash_key(const std::string &key) {
			return hasher(key);
		}

		///todo： change type string value to row_t value;
		RC  			Read(const std::string &key, std::string &value_out, table_t * table = nullptr);
		RC				Write(const std::string &key, const std::string &value, table_t * table = nullptr);

		unordered_map<uint64_t, uint64_t> record_pos; // hash_key -> latest write


	private:
		// insert rows
		uint64_t 		insert_cnt;
		PendingTxnInsert insert_rows[MAX_ROW_PER_TXN];
		txnid_t 		txn_id;
		ts_t 			timestamp;

		bool _write_copy_ptr;
		TreeTxnState *tree_state;
		std::string last_tree_conflict_summary;

		static std::hash<std::string> hasher;
		void reset_tree_state();
		RC tree_commit_impl(bool preserve_on_abort);

	};
}



#endif //TXN_H
