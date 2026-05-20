//
// Created by zwx on 3/11/26.
//

#include "helper.h"
#include "global.h"
namespace storage
{
    class row_t;
    class txn_man;

    class Manager {
    public:
        void 			init();
        // returns the next timestamp.
        ts_t			get_ts(uint64_t thread_id);

        txn_man * 		get_txn_man(int thd_id) { return _all_txns[thd_id]; };
        void 			set_txn_man(txn_man * txn);

    private:
        pthread_mutex_t ts_mutex;
        uint64_t *		timestamp;
        pthread_mutex_t mutexes[BUCKET_CNT];
        uint64_t 		hash(row_t * row);
        ts_t volatile * volatile * volatile all_ts;
        txn_man ** 		_all_txns;
    };
}
