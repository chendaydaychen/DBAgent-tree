#include "tree_txn.h"

#include <algorithm>
#include <map>
#include <set>

namespace storage {

const BranchState *get_branch_state(const TreeTxnState *state, uint32_t branch_id) {
    if (state == nullptr) {
        return nullptr;
    }
    auto it = state->branches.find(branch_id);
    if (it == state->branches.end()) {
        return nullptr;
    }
    return &it->second;
}

BranchState *get_branch_state(TreeTxnState *state, uint32_t branch_id) {
    if (state == nullptr) {
        return nullptr;
    }
    auto it = state->branches.find(branch_id);
    if (it == state->branches.end()) {
        return nullptr;
    }
    return &it->second;
}

const StagedWrite *find_visible_write(const TreeTxnState *state, uint32_t branch_id, const std::string &key) {
    const BranchState *branch = get_branch_state(state, branch_id);
    while (branch != nullptr) {
        auto it = branch->staged_writes.find(key);
        if (it != branch->staged_writes.end()) {
            return &it->second;
        }
        if (branch->branch_id == branch->parent_branch_id) {
            break;
        }
        branch = get_branch_state(state, branch->parent_branch_id);
    }
    return nullptr;
}

namespace {

bool collect_branch_path(const TreeTxnState *state, uint32_t winner_branch_id, std::vector<const BranchState *> &path) {
    const BranchState *winner = get_branch_state(state, winner_branch_id);
    if (winner == nullptr) {
        return false;
    }
    const BranchState *root = get_branch_state(state, state->root_branch_id);
    if (root == nullptr) {
        return false;
    }

    path.push_back(root);
    if (winner_branch_id != state->root_branch_id) {
        if (winner->parent_branch_id != state->root_branch_id) {
            return false;
        }
        path.push_back(winner);
    }
    return true;
}

} // namespace

bool build_tree_commit_plan(const TreeTxnState *state, TreeCommitPlan &plan) {
    if (state == nullptr || !state->active || state->winner_branch_id == UINT32_MAX) {
        return false;
    }

    std::vector<const BranchState *> path;
    if (!collect_branch_path(state, state->winner_branch_id, path)) {
        return false;
    }

    plan = TreeCommitPlan{};
    plan.winner_branch_id = state->winner_branch_id;

    std::unordered_map<std::string, StagedWrite> merged_writes;
    std::map<std::pair<std::string, uint64_t>, NormalReadDep> merged_reads;
    std::set<std::pair<uint64_t, std::string>> boundary_seen;

    for (const BranchState *branch : path) {
        for (const auto &entry : branch->staged_writes) {
            merged_writes[entry.first] = entry.second;
        }
        for (const auto &dep : branch->normal_reads) {
            auto key = std::make_pair(dep.table_name, dep.primary_key);
            auto it = merged_reads.find(key);
            if (it == merged_reads.end()) {
                merged_reads.emplace(key, dep);
                continue;
            }
            NormalReadDep &current = it->second;
            const bool upgrade_to_strict =
                current.read_kind == TreeReadKind::EXPLORATORY_READ
                && dep.read_kind == TreeReadKind::STRICT_READ;
            const bool newer_same_kind =
                current.read_kind == dep.read_kind
                && dep.read_wts >= current.read_wts;
            if (upgrade_to_strict || newer_same_kind) {
                current = dep;
            }
        }
        for (const auto &dep : branch->boundary_reads) {
            auto key = std::make_pair(dep.key_hash, dep.key_str);
            if (boundary_seen.insert(key).second) {
                plan.boundary_reads.push_back(dep);
            }
        }
    }

    plan.normal_reads.reserve(merged_reads.size());
    for (const auto &entry : merged_reads) {
        plan.normal_reads.push_back(entry.second);
    }

    plan.final_writes.reserve(merged_writes.size());
    for (const auto &entry : merged_writes) {
        plan.final_writes.push_back(entry.second);
    }
    std::sort(plan.final_writes.begin(), plan.final_writes.end(),
              [](const StagedWrite &lhs, const StagedWrite &rhs) {
                  if (lhs.key_hash != rhs.key_hash) {
                      return lhs.key_hash < rhs.key_hash;
                  }
                  return lhs.key_str < rhs.key_str;
              });

    return true;
}

} // namespace storage
