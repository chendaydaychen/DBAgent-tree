#pragma once

#include <cstdint>

namespace storage {

struct Stats_thd {
    uint64_t time_query = 0;
    uint64_t run_time = 0;
    uint64_t latency = 0;
    uint64_t txn_cnt = 0;
    uint64_t time_abort = 0;
    uint64_t abort_cnt = 0;
};

class Stats {
public:
    Stats_thd **_stats = nullptr;
    Stats_thd **tmp_stats = nullptr;

    uint64_t time_query = 0;
    uint64_t run_time = 0;
    uint64_t latency = 0;
    uint64_t txn_cnt = 0;
    uint64_t time_abort = 0;
    uint64_t abort_cnt = 0;

    void init(uint64_t) {}
    void clear(uint64_t) {}
    void commit(uint64_t) {}
    void abort(uint64_t) {}
    void add_lat(uint64_t, uint64_t) {}
};

extern Stats stats;

}  // namespace storage
