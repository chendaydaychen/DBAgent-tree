//
// Created by zwx on 4/2/26.
//

#include "kv_api.h"
#include <atomic>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <cstring> // 引入 strncpy, strcmp

#include "manager.h"
#include "occ.h"
#include "System/thread.h"
#include "System/global.h"
#include "System/mem_alloc.h"
#include "DataFormat/table.h"
#include "DataFormat/row.h"
#include "DataFormat/index_hash.h"
#include "test/wl.h"
#include "test/test.h"

namespace server {
using namespace std;

static std::atomic<uint64_t> g_txn_id{1};
storage::workload* g_wl = nullptr;
const std::string DATA_FILE = "../Storage/test/data.txt";
const std::string SCHEMA_FILE = "../Storage/test/data_schema.txt";

// 简单的字符串哈希函数，将 string 转为 uint64_t 作为索引键
uint64_t hash_key(const std::string& key) {
    std::hash<std::string> hasher;
    return hasher(key);
}

// 封装事务上下文，方便在 TxnHandle.impl 中透传
struct TxnContext {
    storage::txn_man* txn;
    storage::thread_t* thd;
    bool tree_mode {false};
};

namespace {

std::string resolve_existing_path(std::initializer_list<const char *> candidates) {
    for (const char * candidate : candidates) {
        if (candidate == nullptr) {
            continue;
        }
        std::filesystem::path path(candidate);
        if (std::filesystem::exists(path)) {
            return path.string();
        }
    }
    return {};
}

void destroy_txn_handle(TxnHandle &h) {
    if (!h.impl) {
        return;
    }
    TxnContext* ctx = static_cast<TxnContext*>(h.impl);
    if (ctx->txn != nullptr) {
        ctx->txn->release();
        storage::mem_allocator.free(ctx->txn, ctx->txn->alloc_size());
        ctx->txn = nullptr;
    }
    delete ctx->thd;
    delete ctx;
    h.impl = nullptr;
}

} // namespace

bool init_engine() {
    std::cout << "[Engine] Initializing KV Engine..." << std::endl;
    g_wl = new storage::TestWorkload();
    g_wl->init();
    storage::glob_manager = (storage::Manager *) _mm_malloc(sizeof(storage::Manager), 64);
    storage::glob_manager->init();
    storage::mem_allocator.init(storage::g_part_cnt, MEM_SIZE / storage::g_part_cnt); ;
    storage::occ_man.init();

    storage::table_t* the_table = g_wl->tables["Data_TABLE"];
    storage::INDEX* the_index = g_wl->indexes["Data_INDEX"];

    if (!the_table || !the_index) {
        std::cerr << "[Engine] Table or Index not found in schema!" << std::endl;
        return false;
    }

    std::string data_path = resolve_existing_path({
        DATA_FILE.c_str(),
        "Storage/test/data.txt",
    });
    std::ifstream infile(data_path);
    if (!infile.is_open()) {
        std::cout << "[Engine] No existing data file found. Starting fresh." << std::endl;
        return true;
    }

    std::string key_str, value_str;
    int loaded_count = 0;

    storage::thread_t* h_thd = new storage::thread_t();
    h_thd->init(0, g_wl);

    while (infile >> key_str >> value_str) {
        uint64_t primary_key = hash_key(key_str);

        storage::txn_man* txn = nullptr;
        g_wl->get_txn_man(txn, h_thd);

        storage::RC rc = storage::RCOK;
        storage::row_t* new_row = nullptr;
        uint64_t row_id;
        int part_id = 0;

        rc = the_table->get_new_row(new_row, part_id, row_id);
        if (rc != storage::RCOK) {
            txn->finish(storage::Abort);
            continue;
        }

        char key_buf[128] = {0};
        std::vector<char> val_buf(MAX_TUPLE_SIZE, 0);
        strncpy(key_buf, key_str.c_str(), 127);
        strncpy(val_buf.data(), value_str.c_str(), MAX_TUPLE_SIZE - 1);

        new_row->set_primary_key(primary_key);
        new_row->set_value(0, key_buf);
        new_row->set_value(1, val_buf.data());

        storage::itemid_t* m_item = (storage::itemid_t*) storage::mem_allocator.alloc(sizeof(storage::itemid_t), part_id);
        m_item->type = storage::DT_row;
        m_item->location = new_row;
        m_item->valid = true;

        rc = the_index->index_insert(primary_key, m_item, part_id);

        if (rc == storage::RCOK) {
            rc = txn->finish(rc);
            if (rc == storage::RCOK || rc == storage::Commit) {
                loaded_count++;
            }
        } else {
            txn->finish(storage::Abort);
        }
    }

    infile.close();
    std::cout << "[Engine] Successfully loaded " << loaded_count << " records." << std::endl;
    return true;
}

Rc begin_txn(TxnHandle &h) {
    TxnContext* ctx = new TxnContext();
    ctx->thd = new storage::thread_t();
    ctx->thd->init(0, g_wl);
    ctx->tree_mode = false;

    g_wl->get_txn_man(ctx->txn, ctx->thd);
    ctx->txn->start_ts = storage::glob_manager->get_ts(ctx->txn->get_thd_id());

    h.id = g_txn_id.fetch_add(1);
    h.impl = ctx;
    return Rc::OK;
}

Rc begin_tree_txn(TxnHandle &h) {
    TxnContext* ctx = new TxnContext();
    ctx->thd = new storage::thread_t();
    ctx->thd->init(0, g_wl);
    ctx->tree_mode = true;

    g_wl->get_txn_man(ctx->txn, ctx->thd);
    storage::RC rc = ctx->txn->tree_begin();
    if (rc != storage::RCOK) {
        delete ctx->thd;
        delete ctx;
        return Rc::ERROR;
    }

    h.id = g_txn_id.fetch_add(1);
    h.impl = ctx;
    return Rc::OK;
}

Rc get(TxnHandle &h, const std::string &key, std::string &value_out) {
    if (!h.impl) return Rc::ERROR;
    TxnContext* ctx = static_cast<TxnContext*>(h.impl);
    if (ctx->tree_mode) return Rc::ERROR;
    storage::RC rc = ctx->txn->Read(key, value_out);
    if (rc == storage::RCOK)
        return Rc::OK;
    if (rc == storage::Abort)
        return Rc::ABORT;
    return Rc::NOT_FOUND;

}

Rc put(TxnHandle &h, const std::string &key, const std::string &value) {
    if (!h.impl) return Rc::ERROR;
    TxnContext* ctx = static_cast<TxnContext*>(h.impl);
    if (ctx->tree_mode) return Rc::ERROR;
    storage::RC rc = ctx->txn->Write(key, value);
    if (rc == storage::RCOK)
        return Rc::OK;
    if (rc == storage::Abort)
        return Rc::ABORT;
    return Rc::ERROR;

}

Rc tree_create_branch(TxnHandle &h, uint32_t parent_branch_id, uint32_t &branch_id_out) {
    branch_id_out = UINT32_MAX;
    if (!h.impl) return Rc::ERROR;
    TxnContext* ctx = static_cast<TxnContext*>(h.impl);
    if (!ctx->tree_mode) return Rc::ERROR;
    storage::RC rc = ctx->txn->tree_create_branch(parent_branch_id, branch_id_out);
    if (rc == storage::RCOK) return Rc::OK;
    if (rc == storage::Abort) return Rc::ABORT;
    return Rc::ERROR;
}

Rc tree_create_branches(TxnHandle &h, uint32_t parent_branch_id, uint32_t count, std::vector<uint32_t> &branch_ids_out) {
    branch_ids_out.clear();
    branch_ids_out.reserve(count);
    for (uint32_t idx = 0; idx < count; idx++) {
        uint32_t branch_id = UINT32_MAX;
        Rc rc = tree_create_branch(h, parent_branch_id, branch_id);
        if (rc != Rc::OK) {
            branch_ids_out.clear();
            return rc;
        }
        branch_ids_out.push_back(branch_id);
    }
    return Rc::OK;
}

Rc tree_get(TxnHandle &h, uint32_t branch_id, const std::string &key, std::string &value_out) {
    if (!h.impl) return Rc::ERROR;
    TxnContext* ctx = static_cast<TxnContext*>(h.impl);
    if (!ctx->tree_mode) return Rc::ERROR;
    storage::RC rc = ctx->txn->tree_read(branch_id, key, value_out);
    if (rc == storage::RCOK) return Rc::OK;
    if (rc == storage::Abort) return Rc::ABORT;
    return Rc::ERROR;
}

Rc tree_get_strict(TxnHandle &h, uint32_t branch_id, const std::string &key, std::string &value_out) {
    if (!h.impl) return Rc::ERROR;
    TxnContext* ctx = static_cast<TxnContext*>(h.impl);
    if (!ctx->tree_mode) return Rc::ERROR;
    storage::RC rc = ctx->txn->tree_read(branch_id, key, value_out, nullptr, true);
    if (rc == storage::RCOK) return Rc::OK;
    if (rc == storage::Abort) return Rc::ABORT;
    return Rc::ERROR;
}

Rc tree_get_many(
    TxnHandle &h,
    const std::vector<std::pair<uint32_t, std::string>> &requests,
    std::vector<std::string> &values_out,
    bool strict
) {
    values_out.clear();
    values_out.reserve(requests.size());
    for (const auto &request : requests) {
        std::string value;
        Rc rc = strict
            ? tree_get_strict(h, request.first, request.second, value)
            : tree_get(h, request.first, request.second, value);
        if (rc != Rc::OK) {
            values_out.clear();
            return rc;
        }
        values_out.push_back(value);
    }
    return Rc::OK;
}

Rc tree_put(TxnHandle &h, uint32_t branch_id, const std::string &key, const std::string &value) {
    if (!h.impl) return Rc::ERROR;
    TxnContext* ctx = static_cast<TxnContext*>(h.impl);
    if (!ctx->tree_mode) return Rc::ERROR;
    storage::RC rc = ctx->txn->tree_write(branch_id, key, value);
    if (rc == storage::RCOK) return Rc::OK;
    if (rc == storage::Abort) return Rc::ABORT;
    return Rc::ERROR;
}

Rc tree_put_many(TxnHandle &h, const std::vector<std::tuple<uint32_t, std::string, std::string>> &requests) {
    for (const auto &request : requests) {
        Rc rc = tree_put(h, std::get<0>(request), std::get<1>(request), std::get<2>(request));
        if (rc != Rc::OK) {
            return rc;
        }
    }
    return Rc::OK;
}

Rc tree_select_winner(TxnHandle &h, uint32_t branch_id) {
    if (!h.impl) return Rc::ERROR;
    TxnContext* ctx = static_cast<TxnContext*>(h.impl);
    if (!ctx->tree_mode) return Rc::ERROR;
    storage::RC rc = ctx->txn->tree_select_winner(branch_id);
    if (rc == storage::RCOK) return Rc::OK;
    if (rc == storage::Abort) return Rc::ABORT;
    return Rc::ERROR;
}

Rc tree_refresh_winner(TxnHandle &h, uint32_t branch_id, const std::string &key, std::string &value_out) {
    if (!h.impl) return Rc::ERROR;
    TxnContext* ctx = static_cast<TxnContext*>(h.impl);
    if (!ctx->tree_mode) return Rc::ERROR;
    storage::RC rc = ctx->txn->tree_read(branch_id, key, value_out, nullptr, true);
    if (rc == storage::RCOK) return Rc::OK;
    if (rc == storage::Abort) return Rc::ABORT;
    return Rc::ERROR;
}

Rc commit(TxnHandle &h) {
    if (!h.impl) return Rc::ERROR;
    TxnContext* ctx = static_cast<TxnContext*>(h.impl);
    if (ctx->tree_mode) return Rc::ERROR;

    storage::RC rc = ctx->txn->finish(storage::RCOK);

    destroy_txn_handle(h);

    if (rc == storage::RCOK || rc == storage::Commit) {
        return Rc::OK;
    }
    return Rc::ABORT;
}

Rc tree_commit(TxnHandle &h) {
    std::string ignored_abort_reason;
    return tree_commit(h, ignored_abort_reason);
}

Rc tree_commit(TxnHandle &h, std::string &abort_reason_out) {
    abort_reason_out.clear();
    if (!h.impl) return Rc::ERROR;
    TxnContext* ctx = static_cast<TxnContext*>(h.impl);
    if (!ctx->tree_mode) return Rc::ERROR;

    storage::RC rc = ctx->txn->tree_commit();
    abort_reason_out = ctx->txn->tree_last_conflict_reason();
    destroy_txn_handle(h);

    if (rc == storage::RCOK || rc == storage::Commit) {
        return Rc::OK;
    }
    if (rc == storage::Abort) {
        return Rc::ABORT;
    }
    return Rc::ERROR;
}

Rc tree_commit_retryable(TxnHandle &h, std::string &abort_reason_out) {
    abort_reason_out.clear();
    if (!h.impl) return Rc::ERROR;
    TxnContext* ctx = static_cast<TxnContext*>(h.impl);
    if (!ctx->tree_mode) return Rc::ERROR;

    storage::RC rc = ctx->txn->tree_commit_retryable();
    abort_reason_out = ctx->txn->tree_last_conflict_reason();

    if (rc == storage::RCOK || rc == storage::Commit) {
        destroy_txn_handle(h);
        return Rc::OK;
    }
    if (rc == storage::Abort) {
        return Rc::ABORT;
    }
    destroy_txn_handle(h);
    return Rc::ERROR;
}

Rc abort_txn(TxnHandle &h) {
    if (!h.impl) return Rc::ERROR;
    TxnContext* ctx = static_cast<TxnContext*>(h.impl);

    if (ctx->tree_mode) {
        (void)ctx->txn->tree_abort();
    } else {
        ctx->txn->finish(storage::Abort);
    }
    destroy_txn_handle(h);

    return Rc::OK;
}

} // namespace server
