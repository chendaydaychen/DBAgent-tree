//
// Created by zwx on 3/11/26.
//

#include "manager.h"
#include "../DataFormat/row.h"
#include "txn.h"
#include "pthread.h"
namespace storage
{
	void Manager::init() {
		timestamp = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
		*timestamp = 1;

		all_ts = (ts_t volatile **) _mm_malloc(sizeof(ts_t *) * g_thread_cnt, 64);
		for (uint32_t i = 0; i < g_thread_cnt; i++)
			all_ts[i] = (ts_t *) _mm_malloc(sizeof(ts_t), 64);

		_all_txns = new txn_man * [g_thread_cnt];
		for (UInt32 i = 0; i < g_thread_cnt; i++) {
			*all_ts[i] = UINT64_MAX;
			_all_txns[i] = NULL;
		}
		for (UInt32 i = 0; i < BUCKET_CNT; i++)
			pthread_mutex_init( &mutexes[i], NULL );
	}

	uint64_t
	Manager::get_ts(uint64_t thread_id) {
		uint64_t time;
		uint64_t starttime = get_sys_clock();
		time = ATOM_FETCH_ADD((*timestamp), 1);
		return time;
	}

	void Manager::set_txn_man(txn_man * txn) {
		int thd_id = txn->get_thd_id();
		_all_txns[thd_id] = txn;
	}

	uint64_t Manager::hash(row_t * row) {
		uint64_t addr = (uint64_t)row / MEM_ALLIGN;
		return (addr * 1103515247 + 12345) % BUCKET_CNT;
	}
}