#include "global.h"
#include "index_hash.h"
#include "mem_alloc.h"
#include "row.h"
#include "table.h"
namespace storage
{
	RC IndexHash::init(uint64_t bucket_cnt, int part_cnt) {
		_bucket_cnt = bucket_cnt;
		_bucket_cnt_per_part = bucket_cnt / part_cnt;
		_buckets = new BucketHeader * [part_cnt];
		for (int i = 0; i < part_cnt; i++) {
			_buckets[i] = (BucketHeader *) _mm_malloc(sizeof(BucketHeader) * _bucket_cnt_per_part, 64);
			for (uint32_t n = 0; n < _bucket_cnt_per_part; n ++)
				_buckets[i][n].init();
		}
		return RCOK;
	}

	RC
	IndexHash::init(table_t * table) {
		int _part_cnt = 1;
		uint64_t _bucket_cnt = BUCKET_CNT;
		init(_bucket_cnt, _part_cnt);
		assert(table != nullptr);
		this->table = table;
		return RCOK;
	}

	RC
	IndexHash::init(int part_cnt, table_t * table, uint64_t bucket_cnt) {
		init(bucket_cnt, part_cnt);
		this->table = table;
		return RCOK;
	}

	bool IndexHash::index_exist(idx_key_t key) {
		assert(false);
	}

	void
	IndexHash::get_latch(BucketHeader * bucket) {
		while (!ATOM_CAS(bucket->locked, false, true)) {}
	}

	void
	IndexHash::release_latch(BucketHeader * bucket) {
		bool ok = ATOM_CAS(bucket->locked, true, false);
		assert(ok);
	}


	RC IndexHash::index_insert(idx_key_t key, itemid_t * item, int part_id) {
		RC rc = RCOK;
		uint64_t bkt_idx = hash(key);
		assert(bkt_idx < _bucket_cnt_per_part);
		BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
		// 1. get the ex latch
		get_latch(cur_bkt);

		// 2. update the latch list
		cur_bkt->insert_item(key, item, part_id);

		// 3. release the latch
		release_latch(cur_bkt);
		return rc;
	}

	RC IndexHash::index_read(idx_key_t key, itemid_t * &item, int part_id) {
		uint64_t bkt_idx = hash(key);
		assert(bkt_idx < _bucket_cnt_per_part);
		BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
		RC rc = RCOK;
		// 1. get the sh latch
		//	get_latch(cur_bkt);
		cur_bkt->read_item(key, item, table->get_table_name());
		// 3. release the latch
		//	release_latch(cur_bkt);
		return rc;

	}

	RC IndexHash::index_read(idx_key_t key, itemid_t * &item,
							int part_id, int thd_id) {
		uint64_t bkt_idx = hash(key);
		assert(bkt_idx < _bucket_cnt_per_part);
		BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
		RC rc = RCOK;
		// 1. get the sh latch
		//	get_latch(cur_bkt);
		cur_bkt->read_item(key, item, table->get_table_name());
		// 3. release the latch
		//	release_latch(cur_bkt);
		return rc;
	}

	BucketHeader * IndexHash::locate_bucket(idx_key_t key, int part_id) {
		uint64_t bkt_idx = hash(key);
		assert(bkt_idx < _bucket_cnt_per_part);
		return &_buckets[part_id][bkt_idx];
	}

	void IndexHash::latch_bucket(BucketHeader * bucket) {
		get_latch(bucket);
	}

	void IndexHash::unlatch_bucket(BucketHeader * bucket) {
		release_latch(bucket);
	}

	bool IndexHash::bucket_contains_exact_key(BucketHeader * bucket, idx_key_t key, const std::string &key_str, const char * tname) {
		(void)tname;
		if (bucket == nullptr) {
			return false;
		}

		for (BucketNode * node = bucket->first_node; node != NULL; node = node->next) {
			if (node->key != key) {
				continue;
			}
			for (itemid_t * item = node->items; item != NULL; item = item->next) {
				auto * row = static_cast<row_t *>(item->location);
				if (row == nullptr) {
					continue;
				}
				char * existing_key = row->get_value(0);
				if (existing_key != nullptr && key_str == existing_key) {
					return true;
				}
			}
		}
		return false;
	}

	/************** BucketHeader Operations ******************/

	void BucketHeader::init() {
		node_cnt = 0;
		first_node = NULL;
		locked = false;
	}

	void BucketHeader::insert_item(idx_key_t key,
			itemid_t * item,
			int part_id)
	{
		BucketNode * cur_node = first_node;
		BucketNode * prev_node = NULL;
		while (cur_node != NULL) {
			if (cur_node->key == key)
				break;
			prev_node = cur_node;
			cur_node = cur_node->next;
		}
		if (cur_node == NULL) {
			BucketNode * new_node = (BucketNode *)
				mem_allocator.alloc(sizeof(BucketNode), part_id );
			new_node->init(key);
			new_node->items = item;
			if (prev_node != NULL) {
				new_node->next = prev_node->next;
				prev_node->next = new_node;
			} else {
				new_node->next = first_node;
				first_node = new_node;
			}
		} else {
			item->next = cur_node->items;
			cur_node->items = item;
		}
	}

	// void BucketHeader::read_item(idx_key_t key, itemid_t * &item, const char * tname)
	// {
	// 	BucketNode * cur_node = first_node;
	// 	while (cur_node != NULL) {
	// 		if (cur_node->key == key)
	// 			break;
	// 		cur_node = cur_node->next;
	// 	}
	// 	M_ASSERT(cur_node->key == key, "Key does not exist!");
	// 	item = cur_node->items;
	// }

	void BucketHeader::read_item(idx_key_t key, itemid_t * &item, const char * tname)
	{
		BucketNode * cur_node = first_node;
		while (cur_node != NULL) {
			if (cur_node->key == key)
				break;
			cur_node = cur_node->next;
		}

		if (cur_node == NULL || cur_node->key != key) {
			item = NULL;
			return;
		}

		item = cur_node->items;
	}
}
