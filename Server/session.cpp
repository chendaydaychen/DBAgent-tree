//
// Created by zwx on 4/3/26.
//

#include "session.h"
#include "server.h"
#include "kv_api.h"

#include <unistd.h>
#include <iomanip>
#include <sstream>
#include <vector>
#include <algorithm>

namespace
{

    static std::vector<std::string> split_ws(const std::string &s) {
        std::istringstream iss(s);
        std::vector<std::string> v;
        std::string t;
        while (iss >> t) v.push_back(t);
        return v;
    }

    static std::string upper(std::string s) {
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return (char)std::toupper(c); });
        return s;
    }

    static bool parse_u32(const std::string &s, uint32_t &value_out) {
        try {
            size_t pos = 0;
            unsigned long v = std::stoul(s, &pos, 10);
            if (pos != s.size()) return false;
            value_out = static_cast<uint32_t>(v);
            return true;
        } catch (...) {
            return false;
        }
    }

    static std::string hex_encode(const std::string &input) {
        static const char *digits = "0123456789abcdef";
        std::string out;
        out.reserve(input.size() * 2);
        for (unsigned char ch : input) {
            out.push_back(digits[(ch >> 4) & 0x0F]);
            out.push_back(digits[ch & 0x0F]);
        }
        return out;
    }

    static bool hex_decode(const std::string &input, std::string &output) {
        output.clear();
        if ((input.size() % 2) != 0) {
            return false;
        }
        output.reserve(input.size() / 2);
        auto hex_value = [](char ch) -> int {
            if (ch >= '0' && ch <= '9') return ch - '0';
            if (ch >= 'a' && ch <= 'f') return 10 + (ch - 'a');
            if (ch >= 'A' && ch <= 'F') return 10 + (ch - 'A');
            return -1;
        };
        for (size_t idx = 0; idx < input.size(); idx += 2) {
            int hi = hex_value(input[idx]);
            int lo = hex_value(input[idx + 1]);
            if (hi < 0 || lo < 0) {
                output.clear();
                return false;
            }
            output.push_back(static_cast<char>((hi << 4) | lo));
        }
        return true;
    }

}

namespace server
{
    void handle_client(int client_fd) {
        TxnHandle tx{};
        bool in_txn = false;
        bool tree_mode = false;

        send_all(client_fd, "WELCOME DataAgentDB\n");

        while (true) {
            bool ok = false;
            std::string line = read_line(client_fd, ok);
            if (!ok) {
                // 断连兜底
                if (in_txn) {
                    abort_txn(tx);
                    in_txn = false;
                    tree_mode = false;
                }
                break;
            }

            auto parts = split_ws(line);
            if (parts.empty()) {
                send_all(client_fd, "ERR EMPTY_CMD\n");
                continue;
            }

            std::string cmd = upper(parts[0]);

            if (cmd == "START") {
                if (in_txn) {
                    send_all(client_fd, "ERR TXN_ALREADY_STARTED\n");
                    continue;
                }
                auto rc = begin_txn(tx);
                if (rc == Rc::OK) {
                    in_txn = true;
                    tree_mode = false;
                    send_all(client_fd, "OK TXN " + std::to_string(tx.id) + "\n");
                } else {
                    send_all(client_fd, "ERR START_FAIL\n");
                }
            } else if (cmd == "TSTART") {
                if (in_txn) {
                    send_all(client_fd, "ERR TXN_ALREADY_STARTED\n");
                    continue;
                }
                auto rc = begin_tree_txn(tx);
                if (rc == Rc::OK) {
                    in_txn = true;
                    tree_mode = true;
                    send_all(client_fd, "OK TREE_TXN " + std::to_string(tx.id) + " ROOT 0\n");
                } else {
                    send_all(client_fd, "ERR TSTART_FAIL\n");
                }
            } else if (cmd == "GET") {
                if (!in_txn) {
                    send_all(client_fd, "ERR NO_ACTIVE_TXN\n");
                    continue;
                }
                if (tree_mode) {
                    send_all(client_fd, "ERR TREE_TXN_USE_TGET\n");
                    continue;
                }
                if (parts.size() != 2) {
                    send_all(client_fd, "ERR USAGE GET <key>\n");
                    continue;
                }
                std::string val;
                auto rc = get(tx, parts[1], val);
                if (rc == Rc::OK) send_all(client_fd, "OK VALUE " + val + "\n");
                else if (rc == Rc::NOT_FOUND) send_all(client_fd, "ERR NOT_FOUND\n");
                else if (rc == Rc::ABORT) { in_txn = false; send_all(client_fd, "ERR ABORT\n"); }
                else send_all(client_fd, "ERR GET_FAIL\n");
            } else if (cmd == "PUT") {
                if (!in_txn) {
                    send_all(client_fd, "ERR NO_ACTIVE_TXN\n");
                    continue;
                }
                if (tree_mode) {
                    send_all(client_fd, "ERR TREE_TXN_USE_TPUT\n");
                    continue;
                }
                if (parts.size() < 3) {
                    send_all(client_fd, "ERR USAGE PUT <key> <value>\n");
                    continue;
                }
                auto pos1 = line.find(' ');
                auto pos2 = (pos1 == std::string::npos) ? std::string::npos : line.find(' ', pos1 + 1);
                std::string key = parts[1];
                std::string value = (pos2 == std::string::npos) ? "" : line.substr(pos2 + 1);

                auto rc = put(tx, key, value);
                if (rc == Rc::OK) send_all(client_fd, "OK\n");
                else if (rc == Rc::ABORT) { in_txn = false; send_all(client_fd, "ERR ABORT\n"); }
                else send_all(client_fd, "ERR PUT_FAIL\n");
            } else if (cmd == "COMMIT") {
                if (!in_txn) {
                    send_all(client_fd, "ERR NO_ACTIVE_TXN\n");
                    continue;
                }
                if (tree_mode) {
                    send_all(client_fd, "ERR TREE_TXN_USE_TCOMMIT\n");
                    continue;
                }
                auto rc = commit(tx);
                in_txn = false;
                tree_mode = false;
                if (rc == Rc::OK) send_all(client_fd, "OK COMMIT\n");
                else if (rc == Rc::ABORT) send_all(client_fd, "ERR ABORT\n");
                else send_all(client_fd, "ERR COMMIT_FAIL\n");
            } else if (cmd == "TBRANCH") {
                if (!in_txn || !tree_mode) {
                    send_all(client_fd, "ERR NO_ACTIVE_TREE_TXN\n");
                    continue;
                }
                if (parts.size() != 2) {
                    send_all(client_fd, "ERR USAGE TBRANCH <parent_branch_id>\n");
                    continue;
                }
                uint32_t parent_branch_id = 0;
                if (!parse_u32(parts[1], parent_branch_id)) {
                    send_all(client_fd, "ERR BAD_BRANCH_ID\n");
                    continue;
                }
                uint32_t branch_id = 0;
                auto rc = tree_create_branch(tx, parent_branch_id, branch_id);
                if (rc == Rc::OK) send_all(client_fd, "OK BRANCH " + std::to_string(branch_id) + "\n");
                else if (rc == Rc::ABORT) { in_txn = false; tree_mode = false; send_all(client_fd, "ERR ABORT\n"); }
                else send_all(client_fd, "ERR TBRANCH_FAIL\n");
            } else if (cmd == "TBRANCHMANY") {
                if (!in_txn || !tree_mode) {
                    send_all(client_fd, "ERR NO_ACTIVE_TREE_TXN\n");
                    continue;
                }
                if (parts.size() != 3) {
                    send_all(client_fd, "ERR USAGE TBRANCHMANY <parent_branch_id> <count>\n");
                    continue;
                }
                uint32_t parent_branch_id = 0;
                uint32_t count = 0;
                if (!parse_u32(parts[1], parent_branch_id) || !parse_u32(parts[2], count)) {
                    send_all(client_fd, "ERR BAD_BRANCH_ARGS\n");
                    continue;
                }
                std::vector<uint32_t> branch_ids;
                auto rc = tree_create_branches(tx, parent_branch_id, count, branch_ids);
                if (rc == Rc::OK) {
                    std::ostringstream oss;
                    oss << "OK BRANCHES";
                    for (uint32_t branch_id : branch_ids) {
                        oss << " " << branch_id;
                    }
                    oss << "\n";
                    send_all(client_fd, oss.str());
                } else if (rc == Rc::ABORT) {
                    in_txn = false;
                    tree_mode = false;
                    send_all(client_fd, "ERR ABORT\n");
                } else {
                    send_all(client_fd, "ERR TBRANCHMANY_FAIL\n");
                }
            } else if (cmd == "TGET" || cmd == "TGETS") {
                if (!in_txn || !tree_mode) {
                    send_all(client_fd, "ERR NO_ACTIVE_TREE_TXN\n");
                    continue;
                }
                if (parts.size() != 3) {
                    send_all(client_fd, "ERR USAGE TGET/TGETS <branch_id> <key>\n");
                    continue;
                }
                uint32_t branch_id = 0;
                if (!parse_u32(parts[1], branch_id)) {
                    send_all(client_fd, "ERR BAD_BRANCH_ID\n");
                    continue;
                }
                std::string val;
                auto rc = (cmd == "TGETS")
                    ? tree_get_strict(tx, branch_id, parts[2], val)
                    : tree_get(tx, branch_id, parts[2], val);
                if (rc == Rc::OK) send_all(client_fd, "OK VALUE " + val + "\n");
                else if (rc == Rc::ABORT) { in_txn = false; tree_mode = false; send_all(client_fd, "ERR ABORT\n"); }
                else send_all(client_fd, cmd == "TGETS" ? "ERR TGETS_FAIL\n" : "ERR TGET_FAIL\n");
            } else if (cmd == "TGETMANY" || cmd == "TGETSMANY") {
                if (!in_txn || !tree_mode) {
                    send_all(client_fd, "ERR NO_ACTIVE_TREE_TXN\n");
                    continue;
                }
                if (parts.size() < 3) {
                    send_all(client_fd, "ERR USAGE TGETMANY/TGETSMANY <count> (<branch_id> <key>)...\n");
                    continue;
                }
                uint32_t count = 0;
                if (!parse_u32(parts[1], count)) {
                    send_all(client_fd, "ERR BAD_COUNT\n");
                    continue;
                }
                if (parts.size() != static_cast<size_t>(2 + count * 2)) {
                    send_all(client_fd, "ERR BAD_TGETMANY_ARITY\n");
                    continue;
                }
                std::vector<std::pair<uint32_t, std::string>> requests;
                requests.reserve(count);
                bool bad_args = false;
                for (uint32_t idx = 0; idx < count; idx++) {
                    uint32_t branch_id = 0;
                    if (!parse_u32(parts[2 + idx * 2], branch_id)) {
                        bad_args = true;
                        break;
                    }
                    requests.emplace_back(branch_id, parts[3 + idx * 2]);
                }
                if (bad_args) {
                    send_all(client_fd, "ERR BAD_BRANCH_ID\n");
                    continue;
                }
                std::vector<std::string> values;
                auto rc = tree_get_many(tx, requests, values, cmd == "TGETSMANY");
                if (rc == Rc::OK) {
                    std::ostringstream oss;
                    oss << "OK VALUES " << values.size();
                    for (size_t idx = 0; idx < requests.size(); idx++) {
                        oss << " " << requests[idx].first << " " << hex_encode(values[idx]);
                    }
                    oss << "\n";
                    send_all(client_fd, oss.str());
                } else if (rc == Rc::ABORT) {
                    in_txn = false;
                    tree_mode = false;
                    send_all(client_fd, "ERR ABORT\n");
                } else {
                    send_all(client_fd, cmd == "TGETSMANY" ? "ERR TGETSMANY_FAIL\n" : "ERR TGETMANY_FAIL\n");
                }
            } else if (cmd == "TPUT") {
                if (!in_txn || !tree_mode) {
                    send_all(client_fd, "ERR NO_ACTIVE_TREE_TXN\n");
                    continue;
                }
                if (parts.size() < 4) {
                    send_all(client_fd, "ERR USAGE TPUT <branch_id> <key> <value>\n");
                    continue;
                }
                uint32_t branch_id = 0;
                if (!parse_u32(parts[1], branch_id)) {
                    send_all(client_fd, "ERR BAD_BRANCH_ID\n");
                    continue;
                }
                auto pos1 = line.find(' ');
                auto pos2 = (pos1 == std::string::npos) ? std::string::npos : line.find(' ', pos1 + 1);
                auto pos3 = (pos2 == std::string::npos) ? std::string::npos : line.find(' ', pos2 + 1);
                std::string key = parts[2];
                std::string value = (pos3 == std::string::npos) ? "" : line.substr(pos3 + 1);
                auto rc = tree_put(tx, branch_id, key, value);
                if (rc == Rc::OK) send_all(client_fd, "OK\n");
                else if (rc == Rc::ABORT) { in_txn = false; tree_mode = false; send_all(client_fd, "ERR ABORT\n"); }
                else send_all(client_fd, "ERR TPUT_FAIL\n");
            } else if (cmd == "TPUTMANY") {
                if (!in_txn || !tree_mode) {
                    send_all(client_fd, "ERR NO_ACTIVE_TREE_TXN\n");
                    continue;
                }
                if (parts.size() < 3) {
                    send_all(client_fd, "ERR USAGE TPUTMANY <count> (<branch_id> <key> <hex_value>)...\n");
                    continue;
                }
                uint32_t count = 0;
                if (!parse_u32(parts[1], count)) {
                    send_all(client_fd, "ERR BAD_COUNT\n");
                    continue;
                }
                if (parts.size() != static_cast<size_t>(2 + count * 3)) {
                    send_all(client_fd, "ERR BAD_TPUTMANY_ARITY\n");
                    continue;
                }
                std::vector<std::tuple<uint32_t, std::string, std::string>> requests;
                requests.reserve(count);
                bool bad_args = false;
                for (uint32_t idx = 0; idx < count; idx++) {
                    uint32_t branch_id = 0;
                    if (!parse_u32(parts[2 + idx * 3], branch_id)) {
                        bad_args = true;
                        break;
                    }
                    std::string decoded_value;
                    if (!hex_decode(parts[4 + idx * 3], decoded_value)) {
                        bad_args = true;
                        break;
                    }
                    requests.emplace_back(branch_id, parts[3 + idx * 3], decoded_value);
                }
                if (bad_args) {
                    send_all(client_fd, "ERR BAD_TPUTMANY_ARGS\n");
                    continue;
                }
                auto rc = tree_put_many(tx, requests);
                if (rc == Rc::OK) {
                    send_all(client_fd, "OK\n");
                } else if (rc == Rc::ABORT) {
                    in_txn = false;
                    tree_mode = false;
                    send_all(client_fd, "ERR ABORT\n");
                } else {
                    send_all(client_fd, "ERR TPUTMANY_FAIL\n");
                }
            } else if (cmd == "TWINNER") {
                if (!in_txn || !tree_mode) {
                    send_all(client_fd, "ERR NO_ACTIVE_TREE_TXN\n");
                    continue;
                }
                if (parts.size() != 2) {
                    send_all(client_fd, "ERR USAGE TWINNER <branch_id>\n");
                    continue;
                }
                uint32_t branch_id = 0;
                if (!parse_u32(parts[1], branch_id)) {
                    send_all(client_fd, "ERR BAD_BRANCH_ID\n");
                    continue;
                }
                auto rc = tree_select_winner(tx, branch_id);
                if (rc == Rc::OK) send_all(client_fd, "OK WINNER\n");
                else if (rc == Rc::ABORT) { in_txn = false; tree_mode = false; send_all(client_fd, "ERR ABORT\n"); }
                else send_all(client_fd, "ERR TWINNER_FAIL\n");
            } else if (cmd == "TREFRESH_WINNER") {
                if (!in_txn || !tree_mode) {
                    send_all(client_fd, "ERR NO_ACTIVE_TREE_TXN\n");
                    continue;
                }
                if (parts.size() != 3) {
                    send_all(client_fd, "ERR USAGE TREFRESH_WINNER <branch_id> <key>\n");
                    continue;
                }
                uint32_t branch_id = 0;
                if (!parse_u32(parts[1], branch_id)) {
                    send_all(client_fd, "ERR BAD_BRANCH_ID\n");
                    continue;
                }
                std::string val;
                auto rc = tree_refresh_winner(tx, branch_id, parts[2], val);
                if (rc == Rc::OK) send_all(client_fd, "OK VALUE " + val + "\n");
                else if (rc == Rc::ABORT) { in_txn = false; tree_mode = false; send_all(client_fd, "ERR ABORT\n"); }
                else send_all(client_fd, "ERR TREFRESH_WINNER_FAIL\n");
            } else if (cmd == "TCOMMIT") {
                if (!in_txn || !tree_mode) {
                    send_all(client_fd, "ERR NO_ACTIVE_TREE_TXN\n");
                    continue;
                }
                std::string abort_reason;
                auto rc = tree_commit(tx, abort_reason);
                in_txn = false;
                tree_mode = false;
                if (rc == Rc::OK) send_all(client_fd, "OK COMMIT\n");
                else if (rc == Rc::ABORT) {
                    if (!abort_reason.empty()) {
                        send_all(client_fd, "ERR ABORT " + abort_reason + "\n");
                    } else {
                        send_all(client_fd, "ERR ABORT\n");
                    }
                }
                else send_all(client_fd, "ERR TCOMMIT_FAIL\n");
            } else if (cmd == "TCOMMIT_RETRY") {
                if (!in_txn || !tree_mode) {
                    send_all(client_fd, "ERR NO_ACTIVE_TREE_TXN\n");
                    continue;
                }
                std::string abort_reason;
                auto rc = tree_commit_retryable(tx, abort_reason);
                if (rc == Rc::OK) {
                    in_txn = false;
                    tree_mode = false;
                    send_all(client_fd, "OK COMMIT\n");
                } else if (rc == Rc::ABORT) {
                    if (!abort_reason.empty()) {
                        send_all(client_fd, "ERR ABORT " + abort_reason + "\n");
                    } else {
                        send_all(client_fd, "ERR ABORT\n");
                    }
                } else {
                    in_txn = false;
                    tree_mode = false;
                    send_all(client_fd, "ERR TCOMMIT_RETRY_FAIL\n");
                }
            } else if (cmd == "ABORT") {
                if (!in_txn) {
                    send_all(client_fd, "ERR NO_ACTIVE_TXN\n");
                    continue;
                }
                (void)abort_txn(tx);
                in_txn = false;
                tree_mode = false;
                send_all(client_fd, "OK ABORT\n");
            } else if (cmd == "END" || cmd == "QUIT" || cmd == "EXIT") {
                if (in_txn) {
                    abort_txn(tx);
                    in_txn = false;
                    tree_mode = false;
                }
                send_all(client_fd, "BYE\n");
                break;
            } else {
                send_all(client_fd, "ERR BAD_CMD\n");
            }
        }
    }
}
