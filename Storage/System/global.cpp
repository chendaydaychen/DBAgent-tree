//
// Created by zwx on 3/11/26.
//

#include "global.h"
#include "mem_alloc.h"
#include "occ.h"
namespace storage
{
    mem_alloc mem_allocator;
    Manager * glob_manager;
    OptCC occ_man;

    bool volatile warmup_finish = false;
    bool volatile enable_thread_mem_pool = false;
    pthread_barrier_t warmup_bar;


    std::atomic<uint64_t> global_ts_allocator;
    bool g_prt_lat_distr = PRT_LAT_DISTR;
    bool g_part_alloc = PART_ALLOC;
    bool g_mem_pad = MEM_PAD;
    UInt32 g_part_cnt = PART_CNT;
    UInt32 g_thread_cnt = THREAD_CNT;
    bool g_ts_batch_alloc = false;
    UInt32 g_ts_batch_num = 10;

    std::map<string, string> g_params;

    UInt32 g_init_parallelism = INIT_PARALLELISM;

    char * output_file = NULL;



}