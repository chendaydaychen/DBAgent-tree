#pragma once

#ifndef TREE_TXN_H
#define TREE_TXN_H

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace storage {

class row_t;

enum class TreeBranchStatus {
    ACTIVE,
    DROPPED,
    WINNER,
    MERGED
};

enum class TreeReadKind {
    EXPLORATORY_READ,
    STRICT_READ
};

struct NormalReadDep {
    row_t *orig_row {nullptr};
    std::string table_name;
    uint64_t primary_key {0};
    uint64_t read_wts {0};
    TreeReadKind read_kind {TreeReadKind::STRICT_READ};
};

struct BoundaryReadDep {
    uint64_t key_hash {0};
    std::string key_str;
    bool expected_absent {false};
};

enum class StagedWriteKind {
    UPDATE_EXISTING,
    INSERT_ABSENT
};

struct StagedWrite {
    StagedWriteKind kind {StagedWriteKind::UPDATE_EXISTING};
    std::string key_str;
    std::shared_ptr<const std::string> value_ref;
    uint64_t key_hash {0};
    row_t *orig_row {nullptr};
    uint64_t base_wts {0};

    const std::string &value() const {
        static const std::string empty;
        return value_ref ? *value_ref : empty;
    }

    void set_value(std::string value) {
        value_ref = std::make_shared<const std::string>(std::move(value));
    }

    void share_value(const std::shared_ptr<const std::string> &value) {
        value_ref = value;
    }
};

struct BranchState {
    uint32_t branch_id {0};
    uint32_t parent_branch_id {0};
    TreeBranchStatus status {TreeBranchStatus::ACTIVE};
    std::unordered_map<std::string, StagedWrite> staged_writes;
    std::vector<NormalReadDep> normal_reads;
    std::vector<BoundaryReadDep> boundary_reads;
};

struct TreeKeyLookupCacheEntry {
    bool found {false};
    row_t *row {nullptr};
    uint64_t read_wts {0};
    uint64_t key_hash {0};
    bool has_value {false};
    std::string value;
};

struct TreeCommitPlan {
    std::vector<NormalReadDep> normal_reads;
    std::vector<BoundaryReadDep> boundary_reads;
    std::vector<StagedWrite> final_writes;
    uint32_t winner_branch_id {0};
};

struct TreeConflictInfo {
    std::string reason;
    std::string key;
    uint32_t branch_id {UINT32_MAX};

    void clear() {
        reason.clear();
        key.clear();
        branch_id = UINT32_MAX;
    }
};

struct TreeTxnState {
    bool active {false};
    uint32_t root_branch_id {0};
    uint32_t next_branch_id {1};
    uint32_t winner_branch_id {UINT32_MAX};
    std::unordered_map<uint32_t, BranchState> branches;
    std::unordered_map<std::string, TreeKeyLookupCacheEntry> key_lookup_cache;
    TreeConflictInfo last_conflict;
};

const BranchState *get_branch_state(const TreeTxnState *state, uint32_t branch_id);
BranchState *get_branch_state(TreeTxnState *state, uint32_t branch_id);
const StagedWrite *find_visible_write(const TreeTxnState *state, uint32_t branch_id, const std::string &key);
bool build_tree_commit_plan(const TreeTxnState *state, TreeCommitPlan &plan);

} // namespace storage

#endif // TREE_TXN_H
