//
// Created by zwx on 4/2/26.
//

#ifndef KV_API_H
#define KV_API_H


#include <string>
#include <cstdint>
#include <tuple>
#include <utility>
#include <vector>

namespace server
{
    enum class Rc {
        OK = 0,
        NOT_FOUND,
        ABORT,
        ERROR
    };

    // 你现有 txn_man / txn context 的薄包装
    struct TxnHandle {
        void *impl {nullptr};   // 指向你的 txn_man*
        uint64_t id {0};
    };

    bool init_engine();   // main 启动时调用一次

    Rc begin_txn(TxnHandle &h);
    Rc begin_tree_txn(TxnHandle &h);
    Rc get(TxnHandle &h, const std::string &key, std::string &value_out);
    Rc put(TxnHandle &h, const std::string &key, const std::string &value);
    Rc tree_create_branch(TxnHandle &h, uint32_t parent_branch_id, uint32_t &branch_id_out);
    Rc tree_create_branches(TxnHandle &h, uint32_t parent_branch_id, uint32_t count, std::vector<uint32_t> &branch_ids_out);
    Rc tree_get(TxnHandle &h, uint32_t branch_id, const std::string &key, std::string &value_out);
    Rc tree_get_strict(TxnHandle &h, uint32_t branch_id, const std::string &key, std::string &value_out);
    Rc tree_get_many(
        TxnHandle &h,
        const std::vector<std::pair<uint32_t, std::string>> &requests,
        std::vector<std::string> &values_out,
        bool strict = false
    );
    Rc tree_put(TxnHandle &h, uint32_t branch_id, const std::string &key, const std::string &value);
    Rc tree_put_many(TxnHandle &h, const std::vector<std::tuple<uint32_t, std::string, std::string>> &requests);
    Rc tree_select_winner(TxnHandle &h, uint32_t branch_id);
    Rc tree_refresh_winner(TxnHandle &h, uint32_t branch_id, const std::string &key, std::string &value_out);
    Rc commit(TxnHandle &h);
    Rc tree_commit(TxnHandle &h);
    Rc tree_commit(TxnHandle &h, std::string &abort_reason_out);
    Rc tree_commit_retryable(TxnHandle &h, std::string &abort_reason_out);
    Rc abort_txn(TxnHandle &h);
}

#endif //KV_API_H
