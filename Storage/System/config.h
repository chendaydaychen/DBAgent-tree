//
// Created by zwx on 3/11/26.
//

#ifndef CONFIG_H
#define CONFIG_H


/***********************************************/
// Simulation + Hardware
/***********************************************/
#define THREAD_CNT					4
#define PART_CNT					1
#define VIRTUAL_PART_CNT			1
#define PAGE_SIZE					4096
#define CL_SIZE						64
// CPU_FREQ is used to get accurate timing info
#define CPU_FREQ 					2 	// in GHz/s

// # of transactions to run for warmup
#define WARMUP						0
// YCSB or TPCC or TEST
#define WORKLOAD 					TEST

// print the transaction latency distribution
#define PRT_LAT_DISTR				false
#define STATS_ENABLE				true
#define TIME_ENABLE					true

#define MEM_ALLIGN					8

// [THREAD_ALLOC]
#define THREAD_ALLOC				false
#define THREAD_ARENA_SIZE			(1UL << 22)
#define MEM_PAD 					true

// [PART_ALLOC]
#define PART_ALLOC 					false
#define MEM_SIZE					(1UL << 30)
#define NO_FREE						false

/***********************************************/
// Concurrency Control
/***********************************************/
#define CC_ALG 						OCC
#define ISOLATION_LEVEL 			SERIALIZABLE

// all transactions acquire tuples according to the primary key order.
#define KEY_ORDER					false
// transaction roll back changes after abort
#define ROLL_BACK					true
// per-row lock/ts management or central lock/ts management
#define CENTRAL_MAN					false
#define BUCKET_CNT					31
#define ABORT_PENALTY 				100000
#define ABORT_BUFFER_SIZE			10
#define ABORT_BUFFER_ENABLE			true
// [ INDEX ]
#define ENABLE_LATCH				false
#define CENTRAL_INDEX				false
#define CENTRAL_MANAGER 			false
#define INDEX_STRUCT				IDX_HASH
#define BTREE_ORDER 				16


// [OCC]
#define MAX_WRITE_SET				10
#define PER_ROW_VALID				true

/***********************************************/
// Logging
/***********************************************/
#define LOG_COMMAND					false
#define LOG_REDO					false
#define LOG_BATCH_TIME				10 // in ms


/***********************************************/
// Benchmark
/***********************************************/
// max number of rows touched per transaction
#define MAX_ROW_PER_TXN				256
#define QUERY_INTVL 				1UL
#define MAX_TXN_PER_PART 			100000
#define FIRST_PART_LOCAL 			true
#define MAX_TUPLE_SIZE				(128 * 1024) // in bytes
// ==== [YCSB] ====
#define INIT_PARALLELISM			40
#define SYNTH_TABLE_SIZE 			(1024 * 1024 * 10)
#define ZIPF_THETA 					0.6
#define READ_PERC 					0.9
#define WRITE_PERC 					0.1
#define SCAN_PERC 					0
#define SCAN_LEN					20
#define PART_PER_TXN 				1
#define PERC_MULTI_PART				1
#define REQ_PER_QUERY				16
#define FIELD_PER_TUPLE				10
// ==== [TPCC] ====
// For large warehouse count, the tables do not fit in memory
// small tpcc schemas shrink the table size.
#define TPCC_SMALL					false
// Some of the transactions read the data.txt but never use them.
// If TPCC_ACCESS_ALL == fales, then these parts of the transactions
// are not modeled.
#define TPCC_ACCESS_ALL 			false
#define WH_UPDATE					true
#define NUM_WH 						1
//
enum TPCCTxnType {TPCC_ALL,
                TPCC_PAYMENT,
                TPCC_NEW_ORDER,
                TPCC_ORDER_STATUS,
                TPCC_DELIVERY,
                TPCC_STOCK_LEVEL};
extern TPCCTxnType 					g_tpcc_txn_type;

//#define TXN_TYPE					TPCC_ALL
#define PERC_PAYMENT 				0.5
#define FIRSTNAME_MINLEN 			8
#define FIRSTNAME_LEN 				16
#define LASTNAME_LEN 				16

#define DIST_PER_WARE				10

/***********************************************/
// TODO centralized CC management.
/***********************************************/
#define MAX_LOCK_CNT				(20 * THREAD_CNT)
#define TSTAB_SIZE                  50 * THREAD_CNT
#define TSTAB_FREE                  TSTAB_SIZE
#define TSREQ_FREE                  4 * TSTAB_FREE
#define MVHIS_FREE                  4 * TSTAB_FREE
#define SPIN                        false

/***********************************************/
// Test cases
/***********************************************/
#define TEST_ALL					true
enum TestCases {
    READ_WRITE,
    CONFLICT,
    TREE_WINNER_OVERRIDE,
    TREE_SIBLING_ISOLATION,
    TREE_EXPLORATORY_ROOT_READ,
    TREE_WINNER_REFRESH_UPDATE,
    TREE_CONFLICT_ABSENT_INSERT,
    TREE_ABORT_CLEANUP
};
extern TestCases					g_test_case;

/***********************************************/
// DEBUG info
/***********************************************/
#define WL_VERB						true
#define IDX_VERB					false
#define VERB_ALLOC					true

#define DEBUG_LOCK					false
#define DEBUG_TIMESTAMP				false
#define DEBUG_SYNTH					false
#define DEBUG_ASSERT				false
#define DEBUG_CC					false //true

/***********************************************/
// Constant
/***********************************************/
// INDEX_STRUCT
#define IDX_HASH 					1
#define IDX_BTREE					2
// WORKLOAD
#define YCSB						1
#define TPCC						2
#define TEST						3
// Concurrency Control Algorithm
#define NO_WAIT						1
#define WAIT_DIE					2
#define DL_DETECT					3
#define TIMESTAMP					4
#define MVCC						5
#define HSTORE						6
#define OCC							7
#define TICTOC						8
#define SILO						9
#define VLL							10
#define HEKATON 					11
//Isolation Levels
#define SERIALIZABLE				1
#define SNAPSHOT					2
#define REPEATABLE_READ				3
// TIMESTAMP allocation method.
#define TS_MUTEX					1
#define TS_CAS						2
#define TS_HW						3
#define TS_CLOCK					4

#endif //CONFIG_H
