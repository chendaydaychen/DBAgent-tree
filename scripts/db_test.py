#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import socket
import threading
import time
import random
from collections import defaultdict

# =========================
# 配置
# =========================
HOST = "127.0.0.1"
PORT = 19091
EOL = "\n"

# 并发参数
THREADS = 16
TXN_PER_THREAD = 2000

# 热点 key（越小冲突越强）
HOT_KEYS = 8
KEY_PREFIX = "hotk_"

# 网络超时
CONNECT_TIMEOUT = 3
SOCKET_TIMEOUT = 5

# 命令
CMD_START = "START"
CMD_GET = "GET"
CMD_PUT = "PUT"
CMD_COMMIT = "COMMIT"
CMD_ABORT = "ABORT"

# 是否打印调试
DEBUG = False


# =========================
# 协议辅助
# =========================
def dprint(msg):
    if DEBUG:
        print(msg)

def normalize(s):
    return s.strip()

def is_ok(resp):
    r = normalize(resp).upper()
    return r.startswith("OK") or r.startswith("VALUE") or r.startswith("FOUND")

def is_err(resp):
    return normalize(resp).upper().startswith("ERR")

def parse_int_from_resp(resp):
    """
    尽量从响应里提取整数。
    支持如:
    - VALUE hotk_1 123
    - OK 123
    - 123
    """
    txt = normalize(resp)
    parts = txt.split()
    # 从后往前找第一个可解析整数
    for token in reversed(parts):
        try:
            return int(token)
        except Exception:
            pass
    return None

def cmd_start():
    return CMD_START + EOL

def cmd_get(k):
    return "{} {}{}".format(CMD_GET, k, EOL)

def cmd_put(k, v):
    return "{} {} {}{}".format(CMD_PUT, k, v, EOL)

def cmd_commit():
    return CMD_COMMIT + EOL

def cmd_abort():
    return CMD_ABORT + EOL


def parse_args():
    parser = argparse.ArgumentParser(description="Stress test DataAgentDB over the text protocol.")
    parser.add_argument("--host", default=HOST, help="database host")
    parser.add_argument("--port", type=int, default=PORT, help="database port")
    parser.add_argument("--threads", type=int, default=THREADS, help="worker thread count")
    parser.add_argument("--txn-per-thread", type=int, default=TXN_PER_THREAD, help="transactions per worker")
    parser.add_argument("--hot-keys", type=int, default=HOT_KEYS, help="number of hot keys")
    parser.add_argument("--key-prefix", default=KEY_PREFIX, help="hot key prefix")
    parser.add_argument("--connect-timeout", type=float, default=CONNECT_TIMEOUT, help="socket connect timeout in seconds")
    parser.add_argument("--socket-timeout", type=float, default=SOCKET_TIMEOUT, help="socket read/write timeout in seconds")
    parser.add_argument("--debug", action="store_true", help="print protocol traffic")
    return parser.parse_args()


# =========================
# Session 客户端
# =========================
class SessionClient(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = None
        self.buf = b""

    def _readline(self):
        while b"\n" not in self.buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("server closed connection")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\n", 1)
        return line.decode("utf-8", errors="replace")

    def connect(self):
        self.sock = socket.create_connection((self.host, self.port), timeout=CONNECT_TIMEOUT)
        self.sock.settimeout(SOCKET_TIMEOUT)

        # 读取 greeting，避免请求响应错位
        try:
            first = self._readline().strip()
            dprint("[DBG] <<< {}".format(first))
            # 允许是 WELCOME ...；若不是也不硬失败
        except socket.timeout:
            pass

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def request(self, cmd):
        if not self.sock:
            raise RuntimeError("not connected")
        self.sock.sendall(cmd.encode("utf-8"))
        resp = self._readline()
        dprint("[DBG] >>> {}".format(cmd.strip()))
        dprint("[DBG] <<< {}".format(resp.strip()))
        return resp


# =========================
# 统计
# =========================
class Stats(object):
    def __init__(self):
        self.conn_fail = 0
        self.exceptions = 0

        self.start_ok = 0
        self.start_fail = 0
        self.get_ok = 0
        self.get_fail = 0
        self.put_ok = 0
        self.put_fail = 0
        self.commit_ok = 0
        self.commit_fail = 0
        self.rollback_ok = 0
        self.rollback_fail = 0

        self.txn_success = 0
        self.txn_fail = 0

        # 冲突相关
        self.retry_count = 0
        self.abort_count = 0

        self.lat_ms = []

        # 每个 key 成功提交的 +1 次数（用于最终一致性校验）
        self.applied_increments = defaultdict(int)

    def merge(self, o):
        self.conn_fail += o.conn_fail
        self.exceptions += o.exceptions
        self.start_ok += o.start_ok
        self.start_fail += o.start_fail
        self.get_ok += o.get_ok
        self.get_fail += o.get_fail
        self.put_ok += o.put_ok
        self.put_fail += o.put_fail
        self.commit_ok += o.commit_ok
        self.commit_fail += o.commit_fail
        self.rollback_ok += o.rollback_ok
        self.rollback_fail += o.rollback_fail
        self.txn_success += o.txn_success
        self.txn_fail += o.txn_fail
        self.retry_count += o.retry_count
        self.abort_count += o.abort_count
        self.lat_ms.extend(o.lat_ms)
        for k, v in o.applied_increments.items():
            self.applied_increments[k] += v


def percentile(arr, p):
    if not arr:
        return 0.0
    s = sorted(arr)
    idx = int((len(s) - 1) * p)
    return s[idx]


# =========================
# 预加载
# =========================
def preload_keys(keys, init_val=0):
    cli = SessionClient(HOST, PORT)
    cli.connect()
    try:
        # 单事务预加载
        r = cli.request(cmd_start())
        if not is_ok(r):
            raise RuntimeError("preload START failed: {}".format(r.strip()))

        for k in keys:
            r = cli.request(cmd_put(k, str(init_val)))
            if not is_ok(r):
                raise RuntimeError("preload PUT failed key={} resp={}".format(k, r.strip()))

        r = cli.request(cmd_commit())
        if not is_ok(r):
            raise RuntimeError("preload COMMIT failed: {}".format(r.strip()))
    finally:
        cli.close()


# =========================
# 工作者线程：事务 +1
# =========================
def worker(tid, keys, gstats, lock):
    st = Stats()
    rng = random.Random(20260405 + tid)
    cli = SessionClient(HOST, PORT)

    try:
        cli.connect()
    except Exception:
        st.conn_fail += 1
        with lock:
            gstats.merge(st)
        return

    try:
        for _ in range(TXN_PER_THREAD):
            t0 = time.perf_counter()
            key = keys[rng.randint(0, len(keys) - 1)]
            txn_ok = False

            try:
                # START
                r = cli.request(cmd_start())
                if is_ok(r):
                    st.start_ok += 1
                else:
                    st.start_fail += 1
                    st.txn_fail += 1
                    st.abort_count += 1
                    st.lat_ms.append((time.perf_counter() - t0) * 1000.0)
                    continue

                # GET 当前值
                r = cli.request(cmd_get(key))
                cur = parse_int_from_resp(r)
                if cur is None:
                    st.get_fail += 1
                    # 事务失败，回滚
                    rr = cli.request(cmd_abort())
                    if is_ok(rr):
                        st.rollback_ok += 1
                    else:
                        st.rollback_fail += 1
                    st.txn_fail += 1
                    st.abort_count += 1
                    st.lat_ms.append((time.perf_counter() - t0) * 1000.0)
                    continue
                else:
                    st.get_ok += 1

                # PUT +1
                nxt = cur + 1
                r = cli.request(cmd_put(key, str(nxt)))
                if is_ok(r):
                    st.put_ok += 1
                else:
                    st.put_fail += 1
                    rr = cli.request(cmd_abort())
                    if is_ok(rr):
                        st.rollback_ok += 1
                    else:
                        st.rollback_fail += 1
                    st.txn_fail += 1
                    st.abort_count += 1
                    st.lat_ms.append((time.perf_counter() - t0) * 1000.0)
                    continue

                # COMMIT
                r = cli.request(cmd_commit())
                if is_ok(r):
                    st.commit_ok += 1
                    st.txn_success += 1
                    st.applied_increments[key] += 1
                    txn_ok = True
                else:
                    st.commit_fail += 1
                    st.txn_fail += 1
                    st.abort_count += 1

            except Exception:
                st.exceptions += 1
                st.txn_fail += 1
                try:
                    rr = cli.request(cmd_abort())
                    if is_ok(rr):
                        st.rollback_ok += 1
                    else:
                        st.rollback_fail += 1
                except Exception:
                    st.rollback_fail += 1

            finally:
                st.lat_ms.append((time.perf_counter() - t0) * 1000.0)

                # 可选：失败重试统计（这里不自动重试，只计数）
                if not txn_ok:
                    st.retry_count += 1

    finally:
        cli.close()
        with lock:
            gstats.merge(st)


# =========================
# 最终一致性校验
# =========================
def verify_final_values(keys, expected_map, init_val=0):
    """
    读取每个 key 的最终值，与 init_val + expected_increments 对比
    """
    cli = SessionClient(HOST, PORT)
    mismatches = []
    unreadable = []
    try:
        cli.connect()
        r = cli.request(cmd_start())
        if not is_ok(r):
            raise RuntimeError("verify START failed: {}".format(r.strip()))
        for k in keys:
            r = cli.request(cmd_get(k))
            v = parse_int_from_resp(r)
            if v is None:
                unreadable.append((k, r.strip()))
                continue

            expect = init_val + expected_map.get(k, 0)
            if v != expect:
                mismatches.append((k, expect, v))
    finally:
        try:
            if cli.sock:
                cli.request(cmd_abort())
        except Exception:
            pass
        cli.close()

    return mismatches, unreadable


def main():
    global HOST, PORT, THREADS, TXN_PER_THREAD, HOT_KEYS, KEY_PREFIX
    global CONNECT_TIMEOUT, SOCKET_TIMEOUT, DEBUG

    args = parse_args()
    HOST = args.host
    PORT = args.port
    THREADS = args.threads
    TXN_PER_THREAD = args.txn_per_thread
    HOT_KEYS = args.hot_keys
    KEY_PREFIX = args.key_prefix
    CONNECT_TIMEOUT = args.connect_timeout
    SOCKET_TIMEOUT = args.socket_timeout
    DEBUG = args.debug

    print("[INFO] target={}:{}".format(HOST, PORT))
    print("[INFO] threads={}, txn/thread={}".format(THREADS, TXN_PER_THREAD))
    print("[INFO] hot_keys={}".format(HOT_KEYS))

    keys = ["{}{}".format(KEY_PREFIX, i) for i in range(HOT_KEYS)]
    init_val = 0

    # 1) 预加载
    t_pre0 = time.perf_counter()
    preload_keys(keys, init_val=init_val)
    t_pre1 = time.perf_counter()
    print("[INFO] preload done: {} keys, {:.3f}s".format(len(keys), t_pre1 - t_pre0))

    # 2) 并发执行
    gstats = Stats()
    lock = threading.Lock()
    threads = []

    t0 = time.perf_counter()
    for tid in range(THREADS):
        th = threading.Thread(target=worker, args=(tid, keys, gstats, lock))
        threads.append(th)
        th.start()

    for th in threads:
        th.join()
    t1 = time.perf_counter()

    total_txn = THREADS * TXN_PER_THREAD
    elapsed = t1 - t0
    tps = total_txn / elapsed if elapsed > 0 else 0.0

    p50 = percentile(gstats.lat_ms, 0.50)
    p95 = percentile(gstats.lat_ms, 0.95)
    p99 = percentile(gstats.lat_ms, 0.99)

    print("\n===== RESULT =====")
    print("total_txn         : {}".format(total_txn))
    print("elapsed_sec       : {:.3f}".format(elapsed))
    print("TPS               : {:.1f}".format(tps))
    print("start_ok/fail     : {}/{}".format(gstats.start_ok, gstats.start_fail))
    print("get_ok/fail       : {}/{}".format(gstats.get_ok, gstats.get_fail))
    print("put_ok/fail       : {}/{}".format(gstats.put_ok, gstats.put_fail))
    print("commit_ok/fail    : {}/{}".format(gstats.commit_ok, gstats.commit_fail))
    print("rollback_ok/fail  : {}/{}".format(gstats.rollback_ok, gstats.rollback_fail))
    print("txn_success/fail  : {}/{}".format(gstats.txn_success, gstats.txn_fail))
    print("conn_fail         : {}".format(gstats.conn_fail))
    print("exceptions        : {}".format(gstats.exceptions))
    print("retry_count       : {}".format(gstats.retry_count))
    print("abort_count       : {}".format(gstats.abort_count))
    print("txn_latency_p50ms : {:.3f}".format(p50))
    print("txn_latency_p95ms : {:.3f}".format(p95))
    print("txn_latency_p99ms : {:.3f}".format(p99))

    # 3) 一致性校验（检查丢失更新）
    mismatches, unreadable = verify_final_values(keys, gstats.applied_increments, init_val=init_val)

    print("\n===== VERIFY =====")
    expected_total_inc = sum(gstats.applied_increments.values())
    print("expected_total_inc: {}".format(expected_total_inc))

    if unreadable:
        print("[WARN] unreadable keys: {}".format(len(unreadable)))
        for x in unreadable[:10]:
            print("  key={} resp={}".format(x[0], x[1]))

    if mismatches:
        print("[FAIL] mismatch keys: {}".format(len(mismatches)))
        for k, e, a in mismatches[:20]:
            print("  key={} expect={} actual={}".format(k, e, a))
    else:
        print("[PASS] final value check passed (no lost-update observed on sampled keys)")

    # 额外总结
    hard_fail = gstats.conn_fail + gstats.exceptions
    if hard_fail > 0:
        print("[WARN] hard failures detected: {}".format(hard_fail))


if __name__ == "__main__":
    main()
