#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import random
import socket
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import DefaultDict, Dict, List, Optional, Sequence, Tuple


HOST = "127.0.0.1"
PORT = 19091
EOL = "\n"

DEFAULT_WORKERS = 16
DEFAULT_TASKS_PER_WORKER = 200
DEFAULT_MAX_RETRIES = 2
DEFAULT_TASK_SEED = 20260425

DEFAULT_TRADITIONAL_HOT_KEYS = 8

DEFAULT_BRANCHES = 4
DEFAULT_COMMON_KEYS = 8
DEFAULT_BRANCH_RETOUCH_KEYS = 2
DEFAULT_HOT_BUCKETS = 8

CONNECT_TIMEOUT = 3.0
SOCKET_TIMEOUT = 5.0
DEBUG = False


def dprint(msg: str) -> None:
    if DEBUG:
        print(msg)


def normalize(resp: str) -> str:
    return resp.strip()


def is_ok(resp: str) -> bool:
    txt = normalize(resp).upper()
    return txt.startswith("OK") or txt.startswith("VALUE") or txt.startswith("FOUND")


def is_abort(resp: str) -> bool:
    return "ABORT" in normalize(resp).upper()


def parse_abort_reason(resp: str) -> str:
    parts = normalize(resp).split()
    upper_parts = [item.upper() for item in parts]
    if len(parts) >= 3 and upper_parts[0] == "ERR" and upper_parts[1] == "ABORT":
        return parts[2]
    if "ABORT" in upper_parts:
        return "UNKNOWN"
    return ""


def record_abort_reason(stats: "Stats", resp: str) -> None:
    reason = parse_abort_reason(resp)
    if reason:
        stats.extra["abort_reason_{}".format(reason)] += 1


def parse_int_from_resp(resp: str) -> Optional[int]:
    txt = normalize(resp)
    parts = txt.split()
    for token in reversed(parts):
        try:
            return int(token)
        except Exception:
            pass
    return None


def percentile(values: Sequence[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int((len(ordered) - 1) * p)
    return ordered[idx]


class SessionClient:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.sock: Optional[socket.socket] = None
        self.buf = b""

    def _readline(self) -> str:
        while b"\n" not in self.buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("server closed connection")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\n", 1)
        return line.decode("utf-8", errors="replace")

    def connect(self) -> None:
        self.sock = socket.create_connection((self.host, self.port), timeout=CONNECT_TIMEOUT)
        self.sock.settimeout(SOCKET_TIMEOUT)
        try:
            greeting = self._readline().strip()
            dprint("[DBG] <<< {}".format(greeting))
        except socket.timeout:
            pass

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
            self.buf = b""

    def request(self, cmd: str) -> str:
        if self.sock is None:
            raise RuntimeError("not connected")
        dprint("[DBG] >>> {}".format(cmd.strip()))
        self.sock.sendall(cmd.encode("utf-8"))
        resp = self._readline()
        dprint("[DBG] <<< {}".format(resp.strip()))
        return resp


def cmd_start() -> str:
    return "START" + EOL


def cmd_get(key: str) -> str:
    return "GET {}{}".format(key, EOL)


def cmd_put(key: str, value: str) -> str:
    return "PUT {} {}{}".format(key, value, EOL)


def cmd_commit() -> str:
    return "COMMIT" + EOL


def cmd_abort() -> str:
    return "ABORT" + EOL


def cmd_tstart() -> str:
    return "TSTART" + EOL


def cmd_tbranch(parent_branch_id: int) -> str:
    return "TBRANCH {}{}".format(parent_branch_id, EOL)


def cmd_tget(branch_id: int, key: str) -> str:
    return "TGET {} {}{}".format(branch_id, key, EOL)


def cmd_tgets(branch_id: int, key: str) -> str:
    return "TGETS {} {}{}".format(branch_id, key, EOL)


def cmd_tput(branch_id: int, key: str, value: str) -> str:
    return "TPUT {} {} {}{}".format(branch_id, key, value, EOL)


def cmd_twinner(branch_id: int) -> str:
    return "TWINNER {}{}".format(branch_id, EOL)


def cmd_tcommit() -> str:
    return "TCOMMIT" + EOL


def parse_branch_id_from_resp(resp: str) -> Optional[int]:
    txt = normalize(resp)
    parts = txt.split()
    for idx, token in enumerate(parts):
        if token.upper() == "BRANCH" and idx + 1 < len(parts):
            try:
                return int(parts[idx + 1])
            except Exception:
                return None
    return None


def safe_abort(cli: SessionClient) -> bool:
    try:
        return is_ok(cli.request(cmd_abort()))
    except Exception:
        return False


def abort_or_raise(cli: SessionClient, stats: Stats) -> None:
    if safe_abort(cli):
        stats.extra["abort_ok"] += 1
        return
    stats.extra["abort_fail"] += 1
    raise ConnectionError("ABORT failed")


@dataclass
class VerifyResult:
    expected_total_inc: int
    mismatches: List[Tuple[str, int, int]]
    unreadable: List[Tuple[str, str]]
    unavailable: bool = False
    unavailable_reason: str = ""


class Stats:
    def __init__(self) -> None:
        self.conn_fail = 0
        self.exceptions = 0
        self.logical_success = 0
        self.logical_fail = 0
        self.retry_count = 0
        self.lat_ms: List[float] = []
        self.applied_increments: DefaultDict[str, int] = defaultdict(int)
        self.extra: DefaultDict[str, int] = defaultdict(int)
        self.unavailable = False
        self.unavailable_reason = ""

    def merge(self, other: "Stats") -> None:
        self.conn_fail += other.conn_fail
        self.exceptions += other.exceptions
        self.logical_success += other.logical_success
        self.logical_fail += other.logical_fail
        self.retry_count += other.retry_count
        self.lat_ms.extend(other.lat_ms)
        for key, value in other.applied_increments.items():
            self.applied_increments[key] += value
        for key, value in other.extra.items():
            self.extra[key] += value


@dataclass
class RunResult:
    scenario: str
    mode: str
    stats: Stats
    verify: VerifyResult
    elapsed_sec: float
    total_tasks: int
    verify_keys: List[str]
    extra_info: List[str]

    @property
    def label(self) -> str:
        return "{}:{}".format(self.scenario, self.mode)


@dataclass
class AgentBranchPlan:
    bucket_id: int
    hint_key: str
    retouch_keys: List[str]


def logical_throughput(result: RunResult) -> float:
    if result.elapsed_sec <= 0:
        return 0.0
    return result.stats.logical_success / result.elapsed_sec


def attempt_count(result: RunResult) -> int:
    extra = result.stats.extra
    if result.scenario == "traditional_txn":
        return extra.get("txn_attempts", 0)
    if result.scenario == "agent_probing":
        if result.mode == "tree":
            return extra.get("tree_txn_attempts", 0)
        return extra.get("probe_txn_attempts", 0) + extra.get("final_txn_attempts", 0)
    if result.scenario == "agent_speculative":
        if result.mode == "tree":
            return extra.get("tree_spec_txn_attempts", 0)
        return extra.get("spec_probe_txn_attempts", 0) + extra.get("final_txn_attempts", 0)
    if result.scenario == "agent_spec_shortcert":
        if result.mode == "tree":
            return extra.get("tree_spec_txn_attempts", 0)
        return extra.get("spec_probe_txn_attempts", 0) + extra.get("final_sc_txn_attempts", 0)
    if result.scenario == "agent_tree_ablation":
        return extra.get("tree_spec_txn_attempts", 0)
    return 0


def attempt_tps(result: RunResult) -> float:
    if result.elapsed_sec <= 0:
        return 0.0
    return attempt_count(result) / result.elapsed_sec


def success_rate(result: RunResult) -> float:
    if result.total_tasks <= 0:
        return 0.0
    return 100.0 * result.stats.logical_success / result.total_tasks


def avg_retry_per_task(result: RunResult) -> float:
    if result.total_tasks <= 0:
        return 0.0
    return result.stats.retry_count / result.total_tasks


def db_style_total_txn(result: RunResult) -> int:
    return attempt_count(result)


def db_style_txn_success(result: RunResult) -> int:
    extra = result.stats.extra
    if result.scenario == "traditional_txn":
        return extra.get("txn_commit_ok", 0)
    if result.scenario == "agent_probing":
        if result.mode == "tree":
            return extra.get("tree_commit_ok", 0)
        return extra.get("probe_abort_ok", 0) + extra.get("final_commit_ok", 0)
    if result.scenario == "agent_speculative":
        if result.mode == "tree":
            return extra.get("tree_spec_commit_ok", 0)
        return extra.get("spec_probe_abort_ok", 0) + extra.get("final_commit_ok", 0)
    if result.scenario == "agent_spec_shortcert":
        if result.mode == "tree":
            return extra.get("tree_spec_commit_ok", 0)
        return extra.get("spec_probe_abort_ok", 0) + extra.get("final_sc_commit_ok", 0)
    if result.scenario == "agent_tree_ablation":
        return extra.get("tree_spec_commit_ok", 0)
    return 0


def db_style_abort_count(result: RunResult) -> int:
    extra = result.stats.extra
    return extra.get("txn_abort_resp", 0) + extra.get("abort_fail", 0)


def db_style_intentional_abort_count(result: RunResult) -> int:
    extra = result.stats.extra
    return extra.get("probe_abort_ok", 0) + extra.get("spec_probe_abort_ok", 0)


def db_style_txn_fail(result: RunResult) -> int:
    total = db_style_total_txn(result)
    success = db_style_txn_success(result)
    return max(0, total - success)


def verify_status(result: RunResult) -> str:
    if result.stats.unavailable or result.verify.unavailable:
        return "UNAVAIL"
    if result.verify.mismatches or result.verify.unreadable:
        return "FAIL"
    return "PASS"


def p95_latency(result: RunResult) -> float:
    return percentile(result.stats.lat_ms, 0.95)


def print_summary_table(results: Sequence[RunResult]) -> None:
    if not results:
        return

    print("\n===== SUMMARY =====")
    headers = [
        ("scenario", 18),
        ("mode", 12),
        ("tasks", 10),
        ("attempt_tps", 12),
        ("throughput", 12),
        ("success%", 10),
        ("retries", 10),
        ("p95_ms", 10),
        ("verify", 8),
    ]
    header_line = " ".join(name.ljust(width) for name, width in headers)
    print(header_line)
    print("-" * len(header_line))

    for result in results:
        if result.stats.unavailable:
            row = [
                result.scenario.ljust(18),
                result.mode.ljust(12),
                str(result.total_tasks).rjust(10),
                "n/a".rjust(12),
                "n/a".rjust(12),
                "n/a".rjust(10),
                "n/a".rjust(10),
                "n/a".rjust(10),
                "UNAVAIL".rjust(8),
            ]
        else:
            row = [
                result.scenario.ljust(18),
                result.mode.ljust(12),
                str(result.total_tasks).rjust(10),
                "{:.1f}".format(attempt_tps(result)).rjust(12),
                "{:.1f}".format(logical_throughput(result)).rjust(12),
                "{:.1f}".format(success_rate(result)).rjust(10),
                "{:.2f}".format(avg_retry_per_task(result)).rjust(10),
                "{:.3f}".format(p95_latency(result)).rjust(10),
                verify_status(result).rjust(8),
            ]
        print(" ".join(row))


def print_run_result(result: RunResult) -> None:
    print("\n===== {} / {} =====".format(result.scenario, result.mode))
    for line in result.extra_info:
        print(line)

    if result.stats.unavailable:
        print("[WARN] benchmark unavailable: {}".format(result.stats.unavailable_reason))
        return

    p50 = percentile(result.stats.lat_ms, 0.50)
    p95 = percentile(result.stats.lat_ms, 0.95)
    p99 = percentile(result.stats.lat_ms, 0.99)

    print("\n===== METRICS =====")
    print("logical_tasks_total  : {}".format(result.total_tasks))
    print("txn_attempts_total   : {}".format(attempt_count(result)))
    print("attempt_tps          : {:.2f} txn/s".format(attempt_tps(result)))
    print("logical_success/fail : {}/{}".format(result.stats.logical_success, result.stats.logical_fail))
    print("logical_success_rate : {:.2f}%".format(success_rate(result)))
    print("logical_throughput   : {:.2f} tasks/s".format(logical_throughput(result)))
    print("elapsed_sec          : {:.3f}".format(result.elapsed_sec))
    print("retry_count          : {}".format(result.stats.retry_count))
    print("avg_retry_per_task   : {:.3f}".format(avg_retry_per_task(result)))
    print("conn_fail            : {}".format(result.stats.conn_fail))
    print("exceptions           : {}".format(result.stats.exceptions))
    print("latency_p50_ms       : {:.3f}".format(p50))
    print("latency_p95_ms       : {:.3f}".format(p95))
    print("latency_p99_ms       : {:.3f}".format(p99))

    print("\n===== DB_STYLE =====")
    print("total_txn            : {}".format(db_style_total_txn(result)))
    print("TPS                  : {:.2f}".format(attempt_tps(result)))
    print("txn_success/fail     : {}/{}".format(db_style_txn_success(result), db_style_txn_fail(result)))
    print("retry_count          : {}".format(result.stats.retry_count))
    print("abort_count          : {}".format(db_style_abort_count(result)))
    print("intentional_aborts   : {}".format(db_style_intentional_abort_count(result)))
    print("txn_latency_p50ms    : {:.3f}".format(p50))
    print("txn_latency_p95ms    : {:.3f}".format(p95))
    print("txn_latency_p99ms    : {:.3f}".format(p99))

    if result.stats.extra:
        print("\n===== INTERNAL =====")
        for key in sorted(result.stats.extra.keys()):
            print("{:<20}: {}".format(key, result.stats.extra[key]))

    print("\n===== VERIFY =====")
    print("expected_total_inc   : {}".format(result.verify.expected_total_inc))
    if result.verify.unavailable:
        print("[WARN] verify unavailable: {}".format(result.verify.unavailable_reason))
        return
    if result.verify.unreadable:
        print("[WARN] unreadable keys: {}".format(len(result.verify.unreadable)))
        for key, resp in result.verify.unreadable[:10]:
            print("  key={} resp={}".format(key, resp))
    if result.verify.mismatches:
        print("[FAIL] mismatch keys: {}".format(len(result.verify.mismatches)))
        for key, expect, actual in result.verify.mismatches[:20]:
            print("  key={} expect={} actual={}".format(key, expect, actual))
    else:
        print("[PASS] final value check passed")


def preload_keys(host: str, port: int, keys: Sequence[str], init_val: int) -> float:
    cli = SessionClient(host, port)
    t0 = time.perf_counter()
    cli.connect()
    try:
        resp = cli.request(cmd_start())
        if not is_ok(resp):
            raise RuntimeError("preload START failed: {}".format(normalize(resp)))
        for key in keys:
            resp = cli.request(cmd_put(key, str(init_val)))
            if not is_ok(resp):
                raise RuntimeError("preload PUT failed key={} resp={}".format(key, normalize(resp)))
        resp = cli.request(cmd_commit())
        if not is_ok(resp):
            raise RuntimeError("preload COMMIT failed: {}".format(normalize(resp)))
    finally:
        cli.close()
    return time.perf_counter() - t0


def verify_final_values(
    host: str,
    port: int,
    keys: Sequence[str],
    expected_map: Dict[str, int],
    init_val: int,
) -> VerifyResult:
    cli = SessionClient(host, port)
    mismatches: List[Tuple[str, int, int]] = []
    unreadable: List[Tuple[str, str]] = []
    try:
        cli.connect()
        resp = cli.request(cmd_start())
        if not is_ok(resp):
            raise RuntimeError("verify START failed: {}".format(normalize(resp)))
        for key in keys:
            resp = cli.request(cmd_get(key))
            value = parse_int_from_resp(resp)
            if value is None:
                unreadable.append((key, normalize(resp)))
                continue
            expected = init_val + expected_map.get(key, 0)
            if value != expected:
                mismatches.append((key, expected, value))
    finally:
        try:
            if cli.sock is not None:
                cli.request(cmd_abort())
        except Exception:
            pass
        cli.close()
    return VerifyResult(
        expected_total_inc=sum(expected_map.values()),
        mismatches=mismatches,
        unreadable=unreadable,
    )


def choose_bucket_ids(rng: random.Random, hot_buckets: int, branches: int) -> List[int]:
    if branches <= hot_buckets:
        return rng.sample(list(range(hot_buckets)), branches)
    return [rng.randint(0, hot_buckets - 1) for _ in range(branches)]


def choose_retouch_keys(rng: random.Random, common_keys: Sequence[str], count: int) -> List[str]:
    if count <= 0 or not common_keys:
        return []
    if count >= len(common_keys):
        return list(common_keys)
    return rng.sample(list(common_keys), count)


def build_agent_task_plan(
    rng: random.Random,
    common_key_names: Sequence[str],
    hint_prefix: str,
    hot_buckets: int,
    branches: int,
    branch_retouch_keys: int,
) -> List[AgentBranchPlan]:
    plans: List[AgentBranchPlan] = []
    for bucket_id in choose_bucket_ids(rng, hot_buckets, branches):
        plans.append(
            AgentBranchPlan(
                bucket_id=bucket_id,
                hint_key="{}{}".format(hint_prefix, bucket_id),
                retouch_keys=choose_retouch_keys(rng, common_key_names, branch_retouch_keys),
            )
        )
    return plans


def tree_get_cmd(branch_id: int, key: str, strict: bool = False) -> str:
    return cmd_tgets(branch_id, key) if strict else cmd_tget(branch_id, key)


def build_agent_task_batches(
    workers: int,
    tasks_per_worker: int,
    seed: int,
    common_key_names: Sequence[str],
    hint_prefix: str,
    hot_buckets: int,
    branches: int,
    branch_retouch_keys: int,
) -> List[List[List[AgentBranchPlan]]]:
    batches: List[List[List[AgentBranchPlan]]] = []
    for tid in range(workers):
        rng = random.Random(seed + tid)
        worker_tasks: List[List[AgentBranchPlan]] = []
        for _ in range(tasks_per_worker):
            worker_tasks.append(
                build_agent_task_plan(
                    rng=rng,
                    common_key_names=common_key_names,
                    hint_prefix=hint_prefix,
                    hot_buckets=hot_buckets,
                    branches=branches,
                    branch_retouch_keys=branch_retouch_keys,
                )
            )
        batches.append(worker_tasks)
    return batches


def execute_plain_increment(cli: SessionClient, key: str, tree_mode: bool, stats: Stats) -> bool:
    active = False
    try:
        stats.extra["txn_attempts"] += 1

        resp = cli.request(cmd_tstart() if tree_mode else cmd_start())
        if not is_ok(resp):
            stats.extra["start_fail"] += 1
            return False
        active = True
        stats.extra["start_ok"] += 1

        resp = cli.request(cmd_tget(0, key) if tree_mode else cmd_get(key))
        current = parse_int_from_resp(resp)
        if current is None:
            stats.extra["read_fail"] += 1
            if active:
                abort_or_raise(cli, stats)
            return False
        stats.extra["read_ok"] += 1

        resp = cli.request(cmd_tput(0, key, str(current + 1)) if tree_mode else cmd_put(key, str(current + 1)))
        if not is_ok(resp):
            stats.extra["write_fail"] += 1
            if is_abort(resp):
                active = False
                stats.extra["txn_abort_resp"] += 1
            elif active:
                abort_or_raise(cli, stats)
            return False
        stats.extra["write_ok"] += 1

        if tree_mode:
            resp = cli.request(cmd_twinner(0))
            if not is_ok(resp):
                stats.extra["winner_fail"] += 1
                if is_abort(resp):
                    active = False
                    stats.extra["txn_abort_resp"] += 1
                elif active:
                    abort_or_raise(cli, stats)
                return False
            stats.extra["winner_ok"] += 1

        resp = cli.request(cmd_tcommit() if tree_mode else cmd_commit())
        active = False
        if is_ok(resp):
            stats.extra["txn_commit_ok"] += 1
            return True
        if is_abort(resp):
            stats.extra["txn_abort_resp"] += 1
            if tree_mode:
                record_abort_reason(stats, resp)
            return False
        stats.extra["txn_commit_fail"] += 1
        return False
    except Exception:
        if active:
            if safe_abort(cli):
                stats.extra["abort_ok"] += 1
            else:
                stats.extra["abort_fail"] += 1
        raise


def execute_probe_txn(
    cli: SessionClient,
    common_key_names: Sequence[str],
    plan: AgentBranchPlan,
    stats: Stats,
) -> Optional[int]:
    active = False
    try:
        stats.extra["probe_txn_attempts"] += 1

        resp = cli.request(cmd_start())
        if not is_ok(resp):
            stats.extra["probe_start_fail"] += 1
            return None
        active = True
        stats.extra["probe_start_ok"] += 1

        for key in common_key_names:
            resp = cli.request(cmd_get(key))
            if parse_int_from_resp(resp) is None:
                stats.extra["probe_shared_read_fail"] += 1
                if active:
                    abort_or_raise(cli, stats)
                return None
            stats.extra["probe_shared_read_ok"] += 1

        resp = cli.request(cmd_get(plan.hint_key))
        hint_value = parse_int_from_resp(resp)
        if hint_value is None:
            stats.extra["probe_hint_read_fail"] += 1
            if active:
                abort_or_raise(cli, stats)
            return None
        stats.extra["probe_hint_read_ok"] += 1

        for key in plan.retouch_keys:
            resp = cli.request(cmd_get(key))
            if parse_int_from_resp(resp) is None:
                stats.extra["probe_retouch_read_fail"] += 1
                if active:
                    abort_or_raise(cli, stats)
                return None
            stats.extra["probe_retouch_read_ok"] += 1

        if safe_abort(cli):
            stats.extra["probe_abort_ok"] += 1
        else:
            stats.extra["probe_abort_fail"] += 1
            raise ConnectionError("probe ABORT failed")
        return hint_value
    except Exception:
        if active:
            if safe_abort(cli):
                stats.extra["abort_ok"] += 1
            else:
                stats.extra["abort_fail"] += 1
        raise


def execute_final_baseline_txn(
    cli: SessionClient,
    common_key_names: Sequence[str],
    winner_plan: AgentBranchPlan,
    stats: Stats,
) -> bool:
    active = False
    try:
        stats.extra["final_txn_attempts"] += 1

        resp = cli.request(cmd_start())
        if not is_ok(resp):
            stats.extra["final_start_fail"] += 1
            return False
        active = True
        stats.extra["final_start_ok"] += 1

        for key in common_key_names:
            resp = cli.request(cmd_get(key))
            if parse_int_from_resp(resp) is None:
                stats.extra["final_shared_read_fail"] += 1
                if active:
                    abort_or_raise(cli, stats)
                return False
            stats.extra["final_shared_read_ok"] += 1

        resp = cli.request(cmd_get(winner_plan.hint_key))
        hint_value = parse_int_from_resp(resp)
        if hint_value is None:
            stats.extra["final_hint_read_fail"] += 1
            if active:
                abort_or_raise(cli, stats)
            return False
        stats.extra["final_hint_read_ok"] += 1

        for key in winner_plan.retouch_keys:
            resp = cli.request(cmd_get(key))
            if parse_int_from_resp(resp) is None:
                stats.extra["final_retouch_read_fail"] += 1
                if active:
                    abort_or_raise(cli, stats)
                return False
            stats.extra["final_retouch_read_ok"] += 1

        resp = cli.request(cmd_put(winner_plan.hint_key, str(hint_value + 1)))
        if not is_ok(resp):
            stats.extra["final_write_fail"] += 1
            if is_abort(resp):
                active = False
                stats.extra["txn_abort_resp"] += 1
            elif active:
                abort_or_raise(cli, stats)
            return False
        stats.extra["final_write_ok"] += 1

        resp = cli.request(cmd_commit())
        active = False
        if is_ok(resp):
            stats.extra["final_commit_ok"] += 1
            return True
        if is_abort(resp):
            stats.extra["txn_abort_resp"] += 1
            return False
        stats.extra["final_commit_fail"] += 1
        return False
    except Exception:
        if active:
            if safe_abort(cli):
                stats.extra["abort_ok"] += 1
            else:
                stats.extra["abort_fail"] += 1
        raise


def execute_final_baseline_shortcert_txn(
    cli: SessionClient,
    winner_plan: AgentBranchPlan,
    stats: Stats,
) -> bool:
    active = False
    try:
        stats.extra["final_sc_txn_attempts"] += 1

        resp = cli.request(cmd_start())
        if not is_ok(resp):
            stats.extra["final_sc_start_fail"] += 1
            return False
        active = True
        stats.extra["final_sc_start_ok"] += 1

        resp = cli.request(cmd_get(winner_plan.hint_key))
        hint_value = parse_int_from_resp(resp)
        if hint_value is None:
            stats.extra["final_sc_hint_read_fail"] += 1
            if active:
                abort_or_raise(cli, stats)
            return False
        stats.extra["final_sc_hint_read_ok"] += 1

        resp = cli.request(cmd_put(winner_plan.hint_key, str(hint_value + 1)))
        if not is_ok(resp):
            stats.extra["final_sc_write_fail"] += 1
            if is_abort(resp):
                active = False
                stats.extra["txn_abort_resp"] += 1
            elif active:
                abort_or_raise(cli, stats)
            return False
        stats.extra["final_sc_write_ok"] += 1

        resp = cli.request(cmd_commit())
        active = False
        if is_ok(resp):
            stats.extra["final_sc_commit_ok"] += 1
            return True
        if is_abort(resp):
            stats.extra["txn_abort_resp"] += 1
            return False
        stats.extra["final_sc_commit_fail"] += 1
        return False
    except Exception:
        if active:
            if safe_abort(cli):
                stats.extra["abort_ok"] += 1
            else:
                stats.extra["abort_fail"] += 1
        raise


def execute_probe_txn_with_write(
    cli: SessionClient,
    common_key_names: Sequence[str],
    plan: AgentBranchPlan,
    stats: Stats,
) -> Optional[int]:
    active = False
    try:
        stats.extra["spec_probe_txn_attempts"] += 1

        resp = cli.request(cmd_start())
        if not is_ok(resp):
            stats.extra["spec_probe_start_fail"] += 1
            return None
        active = True
        stats.extra["spec_probe_start_ok"] += 1

        for key in common_key_names:
            resp = cli.request(cmd_get(key))
            if parse_int_from_resp(resp) is None:
                stats.extra["spec_probe_shared_read_fail"] += 1
                if active:
                    abort_or_raise(cli, stats)
                return None
            stats.extra["spec_probe_shared_read_ok"] += 1

        resp = cli.request(cmd_get(plan.hint_key))
        hint_value = parse_int_from_resp(resp)
        if hint_value is None:
            stats.extra["spec_probe_hint_read_fail"] += 1
            if active:
                abort_or_raise(cli, stats)
            return None
        stats.extra["spec_probe_hint_read_ok"] += 1

        for key in plan.retouch_keys:
            resp = cli.request(cmd_get(key))
            if parse_int_from_resp(resp) is None:
                stats.extra["spec_probe_retouch_read_fail"] += 1
                if active:
                    abort_or_raise(cli, stats)
                return None
            stats.extra["spec_probe_retouch_read_ok"] += 1

        resp = cli.request(cmd_put(plan.hint_key, str(hint_value + 1)))
        if not is_ok(resp):
            stats.extra["spec_probe_write_fail"] += 1
            if is_abort(resp):
                active = False
                stats.extra["txn_abort_resp"] += 1
            elif active:
                abort_or_raise(cli, stats)
            return None
        stats.extra["spec_probe_write_ok"] += 1

        if safe_abort(cli):
            active = False
            stats.extra["spec_probe_abort_ok"] += 1
        else:
            stats.extra["spec_probe_abort_fail"] += 1
            raise ConnectionError("spec probe ABORT failed")
        return hint_value
    except Exception:
        if active:
            if safe_abort(cli):
                stats.extra["abort_ok"] += 1
            else:
                stats.extra["abort_fail"] += 1
        raise


def execute_agent_tree_txn(
    cli: SessionClient,
    common_key_names: Sequence[str],
    branch_plans: Sequence[AgentBranchPlan],
    stats: Stats,
) -> Optional[str]:
    active = False
    try:
        stats.extra["tree_txn_attempts"] += 1

        resp = cli.request(cmd_tstart())
        if not is_ok(resp):
            stats.extra["tree_start_fail"] += 1
            return None
        active = True
        stats.extra["tree_start_ok"] += 1

        for key in common_key_names:
            resp = cli.request(cmd_tget(0, key))
            if parse_int_from_resp(resp) is None:
                stats.extra["tree_root_read_fail"] += 1
                if active:
                    abort_or_raise(cli, stats)
                return None
            stats.extra["tree_root_read_ok"] += 1

        winner_branch_id: Optional[int] = None
        winner_plan: Optional[AgentBranchPlan] = None
        winner_hint_value: Optional[int] = None

        for plan in branch_plans:
            resp = cli.request(cmd_tbranch(0))
            if not is_ok(resp):
                stats.extra["tree_branch_create_fail"] += 1
                if is_abort(resp):
                    active = False
                    stats.extra["txn_abort_resp"] += 1
                elif active:
                    abort_or_raise(cli, stats)
                return None
            branch_id = parse_branch_id_from_resp(resp)
            if branch_id is None:
                stats.extra["tree_branch_parse_fail"] += 1
                if active:
                    abort_or_raise(cli, stats)
                return None
            stats.extra["tree_branches_created"] += 1

            resp = cli.request(cmd_tget(branch_id, plan.hint_key))
            hint_value = parse_int_from_resp(resp)
            if hint_value is None:
                stats.extra["tree_hint_read_fail"] += 1
                if is_abort(resp):
                    active = False
                    stats.extra["txn_abort_resp"] += 1
                elif active:
                    abort_or_raise(cli, stats)
                return None
            stats.extra["tree_hint_read_ok"] += 1

            for key in plan.retouch_keys:
                resp = cli.request(cmd_tget(branch_id, key))
                if parse_int_from_resp(resp) is None:
                    stats.extra["tree_retouch_read_fail"] += 1
                    if is_abort(resp):
                        active = False
                        stats.extra["txn_abort_resp"] += 1
                    elif active:
                        abort_or_raise(cli, stats)
                    return None
                stats.extra["tree_retouch_read_ok"] += 1

            better = (
                winner_hint_value is None
                or hint_value < winner_hint_value
                or (hint_value == winner_hint_value and plan.bucket_id < winner_plan.bucket_id)
            )
            if better:
                winner_branch_id = branch_id
                winner_plan = plan
                winner_hint_value = hint_value

        if winner_branch_id is None or winner_plan is None:
            stats.extra["tree_no_winner"] += 1
            if active:
                abort_or_raise(cli, stats)
            return None

        resp = cli.request(cmd_twinner(winner_branch_id))
        if not is_ok(resp):
            stats.extra["tree_winner_fail"] += 1
            if is_abort(resp):
                active = False
                stats.extra["txn_abort_resp"] += 1
            elif active:
                abort_or_raise(cli, stats)
            return None
        stats.extra["tree_winner_ok"] += 1

        resp = cli.request(cmd_tget(winner_branch_id, winner_plan.hint_key))
        winner_refresh_value = parse_int_from_resp(resp)
        if winner_refresh_value is None:
            stats.extra["tree_winner_refresh_fail"] += 1
            if is_abort(resp):
                active = False
                stats.extra["txn_abort_resp"] += 1
            elif active:
                abort_or_raise(cli, stats)
            return None
        stats.extra["tree_winner_refresh_ok"] += 1

        resp = cli.request(cmd_tput(winner_branch_id, winner_plan.hint_key, str(winner_refresh_value + 1)))
        if not is_ok(resp):
            stats.extra["tree_write_fail"] += 1
            if is_abort(resp):
                active = False
                stats.extra["txn_abort_resp"] += 1
            elif active:
                abort_or_raise(cli, stats)
            return None
        stats.extra["tree_write_ok"] += 1

        resp = cli.request(cmd_tcommit())
        active = False
        if is_ok(resp):
            stats.extra["tree_commit_ok"] += 1
            return winner_plan.hint_key
        if is_abort(resp):
            stats.extra["txn_abort_resp"] += 1
            record_abort_reason(stats, resp)
            return None
        stats.extra["tree_commit_fail"] += 1
        return None
    except Exception:
        if active:
            if safe_abort(cli):
                stats.extra["abort_ok"] += 1
            else:
                stats.extra["abort_fail"] += 1
        raise


def execute_agent_tree_speculative_txn(
    cli: SessionClient,
    common_key_names: Sequence[str],
    branch_plans: Sequence[AgentBranchPlan],
    stats: Stats,
    strict_common: bool = False,
    strict_retouch: bool = False,
) -> Optional[str]:
    active = False
    try:
        stats.extra["tree_spec_txn_attempts"] += 1

        resp = cli.request(cmd_tstart())
        if not is_ok(resp):
            stats.extra["tree_spec_start_fail"] += 1
            return None
        active = True
        stats.extra["tree_spec_start_ok"] += 1

        for key in common_key_names:
            resp = cli.request(tree_get_cmd(0, key, strict=strict_common))
            if parse_int_from_resp(resp) is None:
                stats.extra["tree_spec_root_read_fail"] += 1
                if active:
                    abort_or_raise(cli, stats)
                return None
            stats.extra["tree_spec_root_read_ok"] += 1

        winner_branch_id: Optional[int] = None
        winner_plan: Optional[AgentBranchPlan] = None
        winner_hint_value: Optional[int] = None

        for plan in branch_plans:
            resp = cli.request(cmd_tbranch(0))
            if not is_ok(resp):
                stats.extra["tree_spec_branch_create_fail"] += 1
                if is_abort(resp):
                    active = False
                    stats.extra["txn_abort_resp"] += 1
                elif active:
                    abort_or_raise(cli, stats)
                return None
            branch_id = parse_branch_id_from_resp(resp)
            if branch_id is None:
                stats.extra["tree_spec_branch_parse_fail"] += 1
                if active:
                    abort_or_raise(cli, stats)
                return None
            stats.extra["tree_spec_branches_created"] += 1

            resp = cli.request(cmd_tget(branch_id, plan.hint_key))
            hint_value = parse_int_from_resp(resp)
            if hint_value is None:
                stats.extra["tree_spec_hint_read_fail"] += 1
                if is_abort(resp):
                    active = False
                    stats.extra["txn_abort_resp"] += 1
                elif active:
                    abort_or_raise(cli, stats)
                return None
            stats.extra["tree_spec_hint_read_ok"] += 1

            for key in plan.retouch_keys:
                resp = cli.request(tree_get_cmd(branch_id, key, strict=strict_retouch))
                if parse_int_from_resp(resp) is None:
                    stats.extra["tree_spec_retouch_read_fail"] += 1
                    if is_abort(resp):
                        active = False
                        stats.extra["txn_abort_resp"] += 1
                    elif active:
                        abort_or_raise(cli, stats)
                    return None
                stats.extra["tree_spec_retouch_read_ok"] += 1

            resp = cli.request(cmd_tput(branch_id, plan.hint_key, str(hint_value + 1)))
            if not is_ok(resp):
                stats.extra["tree_spec_write_fail"] += 1
                if is_abort(resp):
                    active = False
                    stats.extra["txn_abort_resp"] += 1
                elif active:
                    abort_or_raise(cli, stats)
                return None
            stats.extra["tree_spec_write_ok"] += 1

            better = (
                winner_hint_value is None
                or hint_value < winner_hint_value
                or (hint_value == winner_hint_value and plan.bucket_id < winner_plan.bucket_id)
            )
            if better:
                winner_branch_id = branch_id
                winner_plan = plan
                winner_hint_value = hint_value

        if winner_branch_id is None or winner_plan is None:
            stats.extra["tree_spec_no_winner"] += 1
            if active:
                abort_or_raise(cli, stats)
            return None

        resp = cli.request(cmd_twinner(winner_branch_id))
        if not is_ok(resp):
            stats.extra["tree_spec_winner_fail"] += 1
            if is_abort(resp):
                active = False
                stats.extra["txn_abort_resp"] += 1
            elif active:
                abort_or_raise(cli, stats)
            return None
        stats.extra["tree_spec_winner_ok"] += 1

        resp = cli.request(cmd_tget(0, winner_plan.hint_key))
        winner_refresh_value = parse_int_from_resp(resp)
        if winner_refresh_value is None:
            stats.extra["tree_spec_winner_refresh_fail"] += 1
            if is_abort(resp):
                active = False
                stats.extra["txn_abort_resp"] += 1
            elif active:
                abort_or_raise(cli, stats)
            return None
        stats.extra["tree_spec_winner_refresh_ok"] += 1

        resp = cli.request(cmd_tput(winner_branch_id, winner_plan.hint_key, str(winner_refresh_value + 1)))
        if not is_ok(resp):
            stats.extra["tree_spec_final_write_fail"] += 1
            if is_abort(resp):
                active = False
                stats.extra["txn_abort_resp"] += 1
            elif active:
                abort_or_raise(cli, stats)
            return None
        stats.extra["tree_spec_final_write_ok"] += 1

        resp = cli.request(cmd_tcommit())
        active = False
        if is_ok(resp):
            stats.extra["tree_spec_commit_ok"] += 1
            return winner_plan.hint_key
        if is_abort(resp):
            stats.extra["txn_abort_resp"] += 1
            record_abort_reason(stats, resp)
            return None
        stats.extra["tree_spec_commit_fail"] += 1
        return None
    except Exception:
        if active:
            if safe_abort(cli):
                stats.extra["abort_ok"] += 1
            else:
                stats.extra["abort_fail"] += 1
        raise


def traditional_worker(
    tid: int,
    host: str,
    port: int,
    hot_keys: Sequence[str],
    tasks_per_worker: int,
    max_retries: int,
    tree_mode: bool,
    global_stats: Stats,
    lock: threading.Lock,
) -> None:
    stats = Stats()
    rng = random.Random(20260420 + tid)
    cli: Optional[SessionClient] = None

    def ensure_client() -> bool:
        nonlocal cli
        if cli is not None and cli.sock is not None:
            return True
        cli = SessionClient(host, port)
        try:
            cli.connect()
            return True
        except Exception:
            stats.conn_fail += 1
            cli.close()
            cli = None
            return False

    try:
        for _ in range(tasks_per_worker):
            task_begin = time.perf_counter()
            key = hot_keys[rng.randint(0, len(hot_keys) - 1)]
            success = False
            retries_used = 0

            for attempt in range(max_retries + 1):
                if not ensure_client():
                    retries_used = attempt + 1
                    continue
                try:
                    committed = execute_plain_increment(cli, key, tree_mode, stats)
                    if committed:
                        stats.logical_success += 1
                        stats.applied_increments[key] += 1
                        success = True
                        retries_used = attempt
                        break
                except Exception:
                    stats.exceptions += 1
                    if cli is not None:
                        cli.close()
                    cli = None
                retries_used = attempt + 1

            if not success:
                stats.logical_fail += 1
            stats.retry_count += min(retries_used, max_retries)
            stats.lat_ms.append((time.perf_counter() - task_begin) * 1000.0)
    finally:
        if cli is not None:
            cli.close()
        with lock:
            global_stats.merge(stats)


def agent_baseline_worker(
    tid: int,
    host: str,
    port: int,
    common_key_names: Sequence[str],
    assigned_tasks: Sequence[Sequence[AgentBranchPlan]],
    tasks_per_worker: int,
    max_retries: int,
    global_stats: Stats,
    lock: threading.Lock,
) -> None:
    stats = Stats()
    cli: Optional[SessionClient] = None

    def ensure_client() -> bool:
        nonlocal cli
        if cli is not None and cli.sock is not None:
            return True
        cli = SessionClient(host, port)
        try:
            cli.connect()
            return True
        except Exception:
            stats.conn_fail += 1
            cli.close()
            cli = None
            return False

    try:
        for task_plans in assigned_tasks:
            task_begin = time.perf_counter()
            success = False
            retries_used = 0

            for attempt in range(max_retries + 1):
                if not ensure_client():
                    retries_used = attempt + 1
                    continue
                try:
                    winner_plan: Optional[AgentBranchPlan] = None
                    winner_value: Optional[int] = None

                    for plan in task_plans:
                        observed = execute_probe_txn(cli, common_key_names, plan, stats)
                        if observed is None:
                            winner_plan = None
                            break
                        better = (
                            winner_value is None
                            or observed < winner_value
                            or (observed == winner_value and plan.bucket_id < winner_plan.bucket_id)
                        )
                        if better:
                            winner_plan = plan
                            winner_value = observed

                    if winner_plan is None:
                        continue

                    if execute_final_baseline_txn(cli, common_key_names, winner_plan, stats):
                        stats.logical_success += 1
                        stats.applied_increments[winner_plan.hint_key] += 1
                        stats.extra["winner_selected"] += 1
                        success = True
                        retries_used = attempt
                        break
                except Exception:
                    stats.exceptions += 1
                    if cli is not None:
                        cli.close()
                    cli = None
                retries_used = attempt + 1

            if not success:
                stats.logical_fail += 1
            stats.retry_count += min(retries_used, max_retries)
            stats.lat_ms.append((time.perf_counter() - task_begin) * 1000.0)
    finally:
        if cli is not None:
            cli.close()
        with lock:
            global_stats.merge(stats)


def agent_speculative_baseline_worker(
    tid: int,
    host: str,
    port: int,
    common_key_names: Sequence[str],
    assigned_tasks: Sequence[Sequence[AgentBranchPlan]],
    tasks_per_worker: int,
    max_retries: int,
    global_stats: Stats,
    lock: threading.Lock,
) -> None:
    stats = Stats()
    cli: Optional[SessionClient] = None

    def ensure_client() -> bool:
        nonlocal cli
        if cli is not None and cli.sock is not None:
            return True
        cli = SessionClient(host, port)
        try:
            cli.connect()
            return True
        except Exception:
            stats.conn_fail += 1
            cli.close()
            cli = None
            return False

    try:
        for task_plans in assigned_tasks:
            task_begin = time.perf_counter()
            success = False
            retries_used = 0

            for attempt in range(max_retries + 1):
                if not ensure_client():
                    retries_used = attempt + 1
                    continue
                try:
                    winner_plan: Optional[AgentBranchPlan] = None
                    winner_value: Optional[int] = None

                    for plan in task_plans:
                        observed = execute_probe_txn_with_write(cli, common_key_names, plan, stats)
                        if observed is None:
                            winner_plan = None
                            break
                        better = (
                            winner_value is None
                            or observed < winner_value
                            or (observed == winner_value and plan.bucket_id < winner_plan.bucket_id)
                        )
                        if better:
                            winner_plan = plan
                            winner_value = observed

                    if winner_plan is None:
                        continue

                    if execute_final_baseline_txn(cli, common_key_names, winner_plan, stats):
                        stats.logical_success += 1
                        stats.applied_increments[winner_plan.hint_key] += 1
                        stats.extra["spec_winner_selected"] += 1
                        success = True
                        retries_used = attempt
                        break
                except Exception:
                    stats.exceptions += 1
                    if cli is not None:
                        cli.close()
                    cli = None
                retries_used = attempt + 1

            if not success:
                stats.logical_fail += 1
            stats.retry_count += min(retries_used, max_retries)
            stats.lat_ms.append((time.perf_counter() - task_begin) * 1000.0)
    finally:
        if cli is not None:
            cli.close()
        with lock:
            global_stats.merge(stats)


def agent_speculative_shortcert_baseline_worker(
    tid: int,
    host: str,
    port: int,
    common_key_names: Sequence[str],
    assigned_tasks: Sequence[Sequence[AgentBranchPlan]],
    tasks_per_worker: int,
    max_retries: int,
    global_stats: Stats,
    lock: threading.Lock,
) -> None:
    stats = Stats()
    cli: Optional[SessionClient] = None

    def ensure_client() -> bool:
        nonlocal cli
        if cli is not None and cli.sock is not None:
            return True
        cli = SessionClient(host, port)
        try:
            cli.connect()
            return True
        except Exception:
            stats.conn_fail += 1
            cli.close()
            cli = None
            return False

    try:
        for task_plans in assigned_tasks:
            task_begin = time.perf_counter()
            success = False
            retries_used = 0

            for attempt in range(max_retries + 1):
                if not ensure_client():
                    retries_used = attempt + 1
                    continue
                try:
                    winner_plan: Optional[AgentBranchPlan] = None
                    winner_value: Optional[int] = None

                    for plan in task_plans:
                        observed = execute_probe_txn_with_write(cli, common_key_names, plan, stats)
                        if observed is None:
                            winner_plan = None
                            break
                        better = (
                            winner_value is None
                            or observed < winner_value
                            or (observed == winner_value and plan.bucket_id < winner_plan.bucket_id)
                        )
                        if better:
                            winner_plan = plan
                            winner_value = observed

                    if winner_plan is None:
                        continue

                    if execute_final_baseline_shortcert_txn(cli, winner_plan, stats):
                        stats.logical_success += 1
                        stats.applied_increments[winner_plan.hint_key] += 1
                        stats.extra["spec_sc_winner_selected"] += 1
                        success = True
                        retries_used = attempt
                        break
                except Exception:
                    stats.exceptions += 1
                    if cli is not None:
                        cli.close()
                    cli = None
                retries_used = attempt + 1

            if not success:
                stats.logical_fail += 1
            stats.retry_count += min(retries_used, max_retries)
            stats.lat_ms.append((time.perf_counter() - task_begin) * 1000.0)
    finally:
        if cli is not None:
            cli.close()
        with lock:
            global_stats.merge(stats)


def agent_speculative_tree_worker(
    tid: int,
    host: str,
    port: int,
    common_key_names: Sequence[str],
    assigned_tasks: Sequence[Sequence[AgentBranchPlan]],
    tasks_per_worker: int,
    max_retries: int,
    global_stats: Stats,
    lock: threading.Lock,
    strict_common: bool = False,
    strict_retouch: bool = False,
) -> None:
    stats = Stats()
    cli: Optional[SessionClient] = None

    def ensure_client() -> bool:
        nonlocal cli
        if cli is not None and cli.sock is not None:
            return True
        cli = SessionClient(host, port)
        try:
            cli.connect()
            return True
        except Exception:
            stats.conn_fail += 1
            cli.close()
            cli = None
            return False

    try:
        for task_plans in assigned_tasks:
            task_begin = time.perf_counter()
            success = False
            retries_used = 0

            for attempt in range(max_retries + 1):
                if not ensure_client():
                    retries_used = attempt + 1
                    continue
                try:
                    committed_key = execute_agent_tree_speculative_txn(
                        cli,
                        common_key_names,
                        task_plans,
                        stats,
                        strict_common=strict_common,
                        strict_retouch=strict_retouch,
                    )
                    if committed_key is not None:
                        stats.logical_success += 1
                        stats.applied_increments[committed_key] += 1
                        success = True
                        retries_used = attempt
                        break
                except Exception:
                    stats.exceptions += 1
                    if cli is not None:
                        cli.close()
                    cli = None
                retries_used = attempt + 1

            if not success:
                stats.logical_fail += 1
            stats.retry_count += min(retries_used, max_retries)
            stats.lat_ms.append((time.perf_counter() - task_begin) * 1000.0)
    finally:
        if cli is not None:
            cli.close()
        with lock:
            global_stats.merge(stats)


def agent_tree_worker(
    tid: int,
    host: str,
    port: int,
    common_key_names: Sequence[str],
    assigned_tasks: Sequence[Sequence[AgentBranchPlan]],
    tasks_per_worker: int,
    max_retries: int,
    global_stats: Stats,
    lock: threading.Lock,
) -> None:
    stats = Stats()
    cli: Optional[SessionClient] = None

    def ensure_client() -> bool:
        nonlocal cli
        if cli is not None and cli.sock is not None:
            return True
        cli = SessionClient(host, port)
        try:
            cli.connect()
            return True
        except Exception:
            stats.conn_fail += 1
            cli.close()
            cli = None
            return False

    try:
        for task_plans in assigned_tasks:
            task_begin = time.perf_counter()
            success = False
            retries_used = 0

            for attempt in range(max_retries + 1):
                if not ensure_client():
                    retries_used = attempt + 1
                    continue
                try:
                    committed_key = execute_agent_tree_txn(cli, common_key_names, task_plans, stats)
                    if committed_key is not None:
                        stats.logical_success += 1
                        stats.applied_increments[committed_key] += 1
                        success = True
                        retries_used = attempt
                        break
                except Exception:
                    stats.exceptions += 1
                    if cli is not None:
                        cli.close()
                    cli = None
                retries_used = attempt + 1

            if not success:
                stats.logical_fail += 1
            stats.retry_count += min(retries_used, max_retries)
            stats.lat_ms.append((time.perf_counter() - task_begin) * 1000.0)
    finally:
        if cli is not None:
            cli.close()
        with lock:
            global_stats.merge(stats)


def run_traditional_txn_benchmark(
    host: str,
    port: int,
    workers: int,
    tasks_per_worker: int,
    hot_keys: int,
    max_retries: int,
    key_prefix: str,
    tree_mode: bool,
) -> RunResult:
    scenario = "traditional_txn"
    mode = "tree" if tree_mode else "baseline"
    hot_key_names = ["{}_trad_hot_{}".format(key_prefix, idx) for idx in range(hot_keys)]
    init_val = 0
    stats = Stats()
    extra_info = [
        "[INFO] target={}:{}".format(host, port),
        "[INFO] scenario={} mode={}".format(scenario, mode),
        "[INFO] workers={}, tasks/worker={}, max_retries={}".format(workers, tasks_per_worker, max_retries),
        "[INFO] hot_keys={}".format(hot_keys),
    ]

    try:
        preload_elapsed = preload_keys(host, port, hot_key_names, init_val)
        extra_info.append("[INFO] preload done: {} keys, {:.3f}s".format(len(hot_key_names), preload_elapsed))

        lock = threading.Lock()
        threads: List[threading.Thread] = []
        t0 = time.perf_counter()
        for tid in range(workers):
            thread = threading.Thread(
                target=traditional_worker,
                args=(tid, host, port, hot_key_names, tasks_per_worker, max_retries, tree_mode, stats, lock),
            )
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
        elapsed = time.perf_counter() - t0
        verify = verify_final_values(host, port, hot_key_names, stats.applied_increments, init_val)
    except Exception as exc:
        stats.unavailable = True
        stats.unavailable_reason = str(exc)
        elapsed = 0.0
        verify = VerifyResult(
            expected_total_inc=0,
            mismatches=[],
            unreadable=[],
            unavailable=True,
            unavailable_reason=str(exc),
        )

    return RunResult(
        scenario=scenario,
        mode=mode,
        stats=stats,
        verify=verify,
        elapsed_sec=elapsed,
        total_tasks=workers * tasks_per_worker,
        verify_keys=hot_key_names,
        extra_info=extra_info,
    )


def run_agent_probing_benchmark(
    host: str,
    port: int,
    workers: int,
    tasks_per_worker: int,
    max_retries: int,
    branches: int,
    common_keys: int,
    branch_retouch_keys: int,
    hot_buckets: int,
    key_prefix: str,
    tree_mode: bool,
    task_seed: int,
) -> RunResult:
    scenario = "agent_probing"
    mode = "tree" if tree_mode else "baseline"
    common_key_names = ["{}_ctx_{}".format(key_prefix, idx) for idx in range(common_keys)]
    hint_prefix = "{}_hint_".format(key_prefix)
    hint_key_names = ["{}{}".format(hint_prefix, idx) for idx in range(hot_buckets)]
    init_val = 0
    stats = Stats()
    extra_info = [
        "[INFO] target={}:{}".format(host, port),
        "[INFO] scenario={} mode={}".format(scenario, mode),
        "[INFO] workers={}, tasks/worker={}, max_retries={}".format(workers, tasks_per_worker, max_retries),
        "[INFO] branches={}, common_keys={}, branch_retouch_keys={}, hot_buckets={}".format(
            branches,
            common_keys,
            branch_retouch_keys,
            hot_buckets,
        ),
    ]

    try:
        preload_elapsed = preload_keys(host, port, list(common_key_names) + hint_key_names, init_val)
        extra_info.append(
            "[INFO] preload done: {} context keys + {} hint buckets, {:.3f}s".format(
                len(common_key_names),
                len(hint_key_names),
                preload_elapsed,
            )
        )
        task_batches = build_agent_task_batches(
            workers=workers,
            tasks_per_worker=tasks_per_worker,
            seed=task_seed,
            common_key_names=common_key_names,
            hint_prefix=hint_prefix,
            hot_buckets=hot_buckets,
            branches=branches,
            branch_retouch_keys=branch_retouch_keys,
        )
        extra_info.append("[INFO] task_seed={}".format(task_seed))

        lock = threading.Lock()
        threads: List[threading.Thread] = []
        t0 = time.perf_counter()
        target = agent_tree_worker if tree_mode else agent_baseline_worker
        for tid in range(workers):
            thread = threading.Thread(
                target=target,
                args=(
                    tid,
                    host,
                    port,
                    common_key_names,
                    task_batches[tid],
                    tasks_per_worker,
                    max_retries,
                    stats,
                    lock,
                ),
            )
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
        elapsed = time.perf_counter() - t0
        verify = verify_final_values(host, port, hint_key_names, stats.applied_increments, init_val)
    except Exception as exc:
        stats.unavailable = True
        stats.unavailable_reason = str(exc)
        elapsed = 0.0
        verify = VerifyResult(
            expected_total_inc=0,
            mismatches=[],
            unreadable=[],
            unavailable=True,
            unavailable_reason=str(exc),
        )

    return RunResult(
        scenario=scenario,
        mode=mode,
        stats=stats,
        verify=verify,
        elapsed_sec=elapsed,
        total_tasks=workers * tasks_per_worker,
        verify_keys=hint_key_names,
        extra_info=extra_info,
    )


def run_agent_speculative_benchmark(
    host: str,
    port: int,
    workers: int,
    tasks_per_worker: int,
    max_retries: int,
    branches: int,
    common_keys: int,
    branch_retouch_keys: int,
    hot_buckets: int,
    key_prefix: str,
    tree_mode: bool,
    task_seed: int,
) -> RunResult:
    scenario = "agent_speculative"
    mode = "tree" if tree_mode else "baseline"
    common_key_names = ["{}_ctx_{}".format(key_prefix, idx) for idx in range(common_keys)]
    hint_prefix = "{}_hint_".format(key_prefix)
    hint_key_names = ["{}{}".format(hint_prefix, idx) for idx in range(hot_buckets)]
    init_val = 0
    stats = Stats()
    extra_info = [
        "[INFO] target={}:{}".format(host, port),
        "[INFO] scenario={} mode={}".format(scenario, mode),
        "[INFO] workers={}, tasks/worker={}, max_retries={}".format(workers, tasks_per_worker, max_retries),
        "[INFO] branches={}, common_keys={}, branch_retouch_keys={}, hot_buckets={}".format(
            branches,
            common_keys,
            branch_retouch_keys,
            hot_buckets,
        ),
        "[INFO] workload=every probe branch writes the real hint key speculatively; TREE keeps loser writes private and refreshes the same winner key after TWINNER",
    ]

    try:
        preload_elapsed = preload_keys(host, port, list(common_key_names) + hint_key_names, init_val)
        extra_info.append(
            "[INFO] preload done: {} context keys + {} hint buckets, {:.3f}s".format(
                len(common_key_names),
                len(hint_key_names),
                preload_elapsed,
            )
        )
        task_batches = build_agent_task_batches(
            workers=workers,
            tasks_per_worker=tasks_per_worker,
            seed=task_seed,
            common_key_names=common_key_names,
            hint_prefix=hint_prefix,
            hot_buckets=hot_buckets,
            branches=branches,
            branch_retouch_keys=branch_retouch_keys,
        )
        extra_info.append("[INFO] task_seed={}".format(task_seed))

        lock = threading.Lock()
        threads: List[threading.Thread] = []
        t0 = time.perf_counter()
        target = agent_speculative_tree_worker if tree_mode else agent_speculative_baseline_worker
        for tid in range(workers):
            thread = threading.Thread(
                target=target,
                args=(
                    tid,
                    host,
                    port,
                    common_key_names,
                    task_batches[tid],
                    tasks_per_worker,
                    max_retries,
                    stats,
                    lock,
                ),
            )
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
        elapsed = time.perf_counter() - t0
        verify = verify_final_values(host, port, hint_key_names, stats.applied_increments, init_val)
    except Exception as exc:
        stats.unavailable = True
        stats.unavailable_reason = str(exc)
        elapsed = 0.0
        verify = VerifyResult(
            expected_total_inc=0,
            mismatches=[],
            unreadable=[],
            unavailable=True,
            unavailable_reason=str(exc),
        )

    return RunResult(
        scenario=scenario,
        mode=mode,
        stats=stats,
        verify=verify,
        elapsed_sec=elapsed,
        total_tasks=workers * tasks_per_worker,
        verify_keys=hint_key_names,
        extra_info=extra_info,
    )


def run_agent_speculative_shortcert_benchmark(
    host: str,
    port: int,
    workers: int,
    tasks_per_worker: int,
    max_retries: int,
    branches: int,
    common_keys: int,
    branch_retouch_keys: int,
    hot_buckets: int,
    key_prefix: str,
    tree_mode: bool,
    task_seed: int,
) -> RunResult:
    scenario = "agent_spec_shortcert"
    mode = "tree" if tree_mode else "baseline_sc"
    common_key_names = ["{}_ctx_{}".format(key_prefix, idx) for idx in range(common_keys)]
    hint_prefix = "{}_hint_".format(key_prefix)
    hint_key_names = ["{}{}".format(hint_prefix, idx) for idx in range(hot_buckets)]
    init_val = 0
    stats = Stats()
    extra_info = [
        "[INFO] target={}:{}".format(host, port),
        "[INFO] scenario={} mode={}".format(scenario, mode),
        "[INFO] workers={}, tasks/worker={}, max_retries={}".format(workers, tasks_per_worker, max_retries),
        "[INFO] branches={}, common_keys={}, branch_retouch_keys={}, hot_buckets={}".format(
            branches,
            common_keys,
            branch_retouch_keys,
            hot_buckets,
        ),
        "[INFO] workload=baseline winner final txn replays only truly strict deps; tree keeps winner-aware short refresh path",
    ]

    try:
        preload_elapsed = preload_keys(host, port, list(common_key_names) + hint_key_names, init_val)
        extra_info.append(
            "[INFO] preload done: {} context keys + {} hint buckets, {:.3f}s".format(
                len(common_key_names),
                len(hint_key_names),
                preload_elapsed,
            )
        )
        task_batches = build_agent_task_batches(
            workers=workers,
            tasks_per_worker=tasks_per_worker,
            seed=task_seed,
            common_key_names=common_key_names,
            hint_prefix=hint_prefix,
            hot_buckets=hot_buckets,
            branches=branches,
            branch_retouch_keys=branch_retouch_keys,
        )
        extra_info.append("[INFO] task_seed={}".format(task_seed))

        lock = threading.Lock()
        threads: List[threading.Thread] = []
        t0 = time.perf_counter()
        target = agent_speculative_tree_worker if tree_mode else agent_speculative_shortcert_baseline_worker
        for tid in range(workers):
            thread = threading.Thread(
                target=target,
                args=(
                    tid,
                    host,
                    port,
                    common_key_names,
                    task_batches[tid],
                    tasks_per_worker,
                    max_retries,
                    stats,
                    lock,
                ),
            )
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
        elapsed = time.perf_counter() - t0
        verify = verify_final_values(host, port, hint_key_names, stats.applied_increments, init_val)
    except Exception as exc:
        stats.unavailable = True
        stats.unavailable_reason = str(exc)
        elapsed = 0.0
        verify = VerifyResult(
            expected_total_inc=0,
            mismatches=[],
            unreadable=[],
            unavailable=True,
            unavailable_reason=str(exc),
        )

    return RunResult(
        scenario=scenario,
        mode=mode,
        stats=stats,
        verify=verify,
        elapsed_sec=elapsed,
        total_tasks=workers * tasks_per_worker,
        verify_keys=hint_key_names,
        extra_info=extra_info,
    )


def run_agent_tree_ablation_benchmark(
    host: str,
    port: int,
    workers: int,
    tasks_per_worker: int,
    max_retries: int,
    branches: int,
    common_keys: int,
    branch_retouch_keys: int,
    hot_buckets: int,
    key_prefix: str,
    task_seed: int,
    strict_common: bool,
    strict_retouch: bool,
    mode_label: str,
) -> RunResult:
    scenario = "agent_tree_ablation"
    mode = mode_label
    common_key_names = ["{}_ctx_{}".format(key_prefix, idx) for idx in range(common_keys)]
    hint_prefix = "{}_hint_".format(key_prefix)
    hint_key_names = ["{}{}".format(hint_prefix, idx) for idx in range(hot_buckets)]
    init_val = 0
    stats = Stats()
    extra_info = [
        "[INFO] target={}:{}".format(host, port),
        "[INFO] scenario={} mode={}".format(scenario, mode),
        "[INFO] workers={}, tasks/worker={}, max_retries={}".format(workers, tasks_per_worker, max_retries),
        "[INFO] branches={}, common_keys={}, branch_retouch_keys={}, hot_buckets={}".format(
            branches,
            common_keys,
            branch_retouch_keys,
            hot_buckets,
        ),
        "[INFO] strict_common={}, strict_retouch={}".format(strict_common, strict_retouch),
    ]

    try:
        preload_elapsed = preload_keys(host, port, list(common_key_names) + hint_key_names, init_val)
        extra_info.append(
            "[INFO] preload done: {} context keys + {} hint buckets, {:.3f}s".format(
                len(common_key_names),
                len(hint_key_names),
                preload_elapsed,
            )
        )
        task_batches = build_agent_task_batches(
            workers=workers,
            tasks_per_worker=tasks_per_worker,
            seed=task_seed,
            common_key_names=common_key_names,
            hint_prefix=hint_prefix,
            hot_buckets=hot_buckets,
            branches=branches,
            branch_retouch_keys=branch_retouch_keys,
        )
        extra_info.append("[INFO] task_seed={}".format(task_seed))

        lock = threading.Lock()
        threads: List[threading.Thread] = []
        t0 = time.perf_counter()
        for tid in range(workers):
            thread = threading.Thread(
                target=agent_speculative_tree_worker,
                args=(
                    tid,
                    host,
                    port,
                    common_key_names,
                    task_batches[tid],
                    tasks_per_worker,
                    max_retries,
                    stats,
                    lock,
                    strict_common,
                    strict_retouch,
                ),
            )
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
        elapsed = time.perf_counter() - t0
        verify = verify_final_values(host, port, hint_key_names, stats.applied_increments, init_val)
    except Exception as exc:
        stats.unavailable = True
        stats.unavailable_reason = str(exc)
        elapsed = 0.0
        verify = VerifyResult(
            expected_total_inc=0,
            mismatches=[],
            unreadable=[],
            unavailable=True,
            unavailable_reason=str(exc),
        )

    return RunResult(
        scenario=scenario,
        mode=mode,
        stats=stats,
        verify=verify,
        elapsed_sec=elapsed,
        total_tasks=workers * tasks_per_worker,
        verify_keys=hint_key_names,
        extra_info=extra_info,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark two workload families on DataAgentDB: "
            "traditional single-path transactions and multi-branch agent probing."
        )
    )
    parser.add_argument(
        "--mode",
        choices=[
            "traditional-baseline",
            "traditional-tree",
            "traditional-compare",
            "agent-baseline",
            "agent-tree",
            "agent-compare",
            "agent-speculative-baseline",
            "agent-speculative-tree",
            "agent-speculative-compare",
            "agent-spec-shortcert-baseline",
            "agent-spec-shortcert-tree",
            "agent-spec-shortcert-compare",
            "agent-tree-ablation",
            "compare-all",
        ],
        default="compare-all",
        help="which workload/protocol comparison to run",
    )
    parser.add_argument("--host", default=HOST, help="database host")
    parser.add_argument("--port", type=int, default=PORT, help="database port")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="concurrent worker count")
    parser.add_argument(
        "--tasks-per-worker",
        type=int,
        default=DEFAULT_TASKS_PER_WORKER,
        help="logical tasks per worker",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help="maximum retry count for each logical task after the first attempt",
    )
    parser.add_argument(
        "--hot-keys",
        type=int,
        default=DEFAULT_TRADITIONAL_HOT_KEYS,
        help="traditional workload hot-key count",
    )
    parser.add_argument("--branches", type=int, default=DEFAULT_BRANCHES, help="candidate branch count per logical task")
    parser.add_argument("--common-keys", type=int, default=DEFAULT_COMMON_KEYS, help="shared context key count")
    parser.add_argument(
        "--branch-retouch-keys",
        type=int,
        default=DEFAULT_BRANCH_RETOUCH_KEYS,
        help="shared context keys revisited inside each branch",
    )
    parser.add_argument(
        "--hot-buckets",
        type=int,
        default=DEFAULT_HOT_BUCKETS,
        help="agent probing hint bucket count",
    )
    parser.add_argument("--key-prefix", default="treebench", help="key prefix used for benchmark data")
    parser.add_argument("--task-seed", type=int, default=DEFAULT_TASK_SEED, help="seed for pre-generated agent task instances")
    parser.add_argument("--connect-timeout", type=float, default=CONNECT_TIMEOUT, help="socket connect timeout in seconds")
    parser.add_argument("--socket-timeout", type=float, default=SOCKET_TIMEOUT, help="socket read timeout in seconds")
    parser.add_argument("--debug", action="store_true", help="print raw protocol traffic")
    return parser.parse_args()


def main() -> None:
    global CONNECT_TIMEOUT, SOCKET_TIMEOUT, DEBUG

    args = parse_args()
    CONNECT_TIMEOUT = args.connect_timeout
    SOCKET_TIMEOUT = args.socket_timeout
    DEBUG = args.debug

    if args.workers <= 0:
        raise SystemExit("--workers must be > 0")
    if args.tasks_per_worker <= 0:
        raise SystemExit("--tasks-per-worker must be > 0")
    if args.max_retries < 0:
        raise SystemExit("--max-retries must be >= 0")
    if args.hot_keys <= 0:
        raise SystemExit("--hot-keys must be > 0")
    if args.branches <= 0:
        raise SystemExit("--branches must be > 0")
    if args.common_keys < 0:
        raise SystemExit("--common-keys must be >= 0")
    if args.branch_retouch_keys < 0:
        raise SystemExit("--branch-retouch-keys must be >= 0")
    if args.hot_buckets <= 0:
        raise SystemExit("--hot-buckets must be > 0")

    results: List[RunResult] = []

    def traditional_run(tree_mode: bool) -> None:
        run = run_traditional_txn_benchmark(
            host=args.host,
            port=args.port,
            workers=args.workers,
            tasks_per_worker=args.tasks_per_worker,
            hot_keys=args.hot_keys,
            max_retries=args.max_retries,
            key_prefix="{}_traditional_{}".format(args.key_prefix, "tree" if tree_mode else "baseline"),
            tree_mode=tree_mode,
        )
        print_run_result(run)
        results.append(run)

    def agent_run(tree_mode: bool) -> None:
        run = run_agent_probing_benchmark(
            host=args.host,
            port=args.port,
            workers=args.workers,
            tasks_per_worker=args.tasks_per_worker,
            max_retries=args.max_retries,
            branches=args.branches,
            common_keys=args.common_keys,
            branch_retouch_keys=args.branch_retouch_keys,
            hot_buckets=args.hot_buckets,
            key_prefix="{}_agent_{}".format(args.key_prefix, "tree" if tree_mode else "baseline"),
            tree_mode=tree_mode,
            task_seed=args.task_seed,
        )
        print_run_result(run)
        results.append(run)

    def agent_speculative_run(tree_mode: bool) -> None:
        run = run_agent_speculative_benchmark(
            host=args.host,
            port=args.port,
            workers=args.workers,
            tasks_per_worker=args.tasks_per_worker,
            max_retries=args.max_retries,
            branches=args.branches,
            common_keys=args.common_keys,
            branch_retouch_keys=args.branch_retouch_keys,
            hot_buckets=args.hot_buckets,
            key_prefix="{}_agent_spec_{}".format(args.key_prefix, "tree" if tree_mode else "baseline"),
            tree_mode=tree_mode,
            task_seed=args.task_seed,
        )
        print_run_result(run)
        results.append(run)

    def agent_spec_shortcert_run(tree_mode: bool) -> None:
        run = run_agent_speculative_shortcert_benchmark(
            host=args.host,
            port=args.port,
            workers=args.workers,
            tasks_per_worker=args.tasks_per_worker,
            max_retries=args.max_retries,
            branches=args.branches,
            common_keys=args.common_keys,
            branch_retouch_keys=args.branch_retouch_keys,
            hot_buckets=args.hot_buckets,
            key_prefix="{}_agent_specsc_{}".format(args.key_prefix, "tree" if tree_mode else "baseline"),
            tree_mode=tree_mode,
            task_seed=args.task_seed,
        )
        print_run_result(run)
        results.append(run)

    def agent_tree_ablation_run() -> None:
        ablations = [
            ("tree_def", False, False),
            ("tree_ctxS", True, False),
            ("tree_rtxS", False, True),
            ("tree_allS", True, True),
        ]
        for mode_label, strict_common, strict_retouch in ablations:
            run = run_agent_tree_ablation_benchmark(
                host=args.host,
                port=args.port,
                workers=args.workers,
                tasks_per_worker=args.tasks_per_worker,
                max_retries=args.max_retries,
                branches=args.branches,
                common_keys=args.common_keys,
                branch_retouch_keys=args.branch_retouch_keys,
                hot_buckets=args.hot_buckets,
                key_prefix="{}_tree_ablate_{}".format(args.key_prefix, mode_label),
                task_seed=args.task_seed,
                strict_common=strict_common,
                strict_retouch=strict_retouch,
                mode_label=mode_label,
            )
            print_run_result(run)
            results.append(run)

    if args.mode == "traditional-baseline":
        traditional_run(tree_mode=False)
    elif args.mode == "traditional-tree":
        traditional_run(tree_mode=True)
    elif args.mode == "traditional-compare":
        traditional_run(tree_mode=False)
        traditional_run(tree_mode=True)
        print_summary_table(results)
    elif args.mode == "agent-baseline":
        agent_run(tree_mode=False)
    elif args.mode == "agent-tree":
        agent_run(tree_mode=True)
    elif args.mode == "agent-compare":
        agent_run(tree_mode=False)
        agent_run(tree_mode=True)
        print_summary_table(results)
    elif args.mode == "agent-speculative-baseline":
        agent_speculative_run(tree_mode=False)
    elif args.mode == "agent-speculative-tree":
        agent_speculative_run(tree_mode=True)
    elif args.mode == "agent-speculative-compare":
        agent_speculative_run(tree_mode=False)
        agent_speculative_run(tree_mode=True)
        print_summary_table(results)
    elif args.mode == "agent-spec-shortcert-baseline":
        agent_spec_shortcert_run(tree_mode=False)
    elif args.mode == "agent-spec-shortcert-tree":
        agent_spec_shortcert_run(tree_mode=True)
    elif args.mode == "agent-spec-shortcert-compare":
        agent_spec_shortcert_run(tree_mode=False)
        agent_spec_shortcert_run(tree_mode=True)
        print_summary_table(results)
    elif args.mode == "agent-tree-ablation":
        agent_tree_ablation_run()
        print_summary_table(results)
    elif args.mode == "compare-all":
        traditional_run(tree_mode=False)
        traditional_run(tree_mode=True)
        agent_run(tree_mode=False)
        agent_run(tree_mode=True)
        agent_speculative_run(tree_mode=False)
        agent_speculative_run(tree_mode=True)
        agent_spec_shortcert_run(tree_mode=False)
        agent_spec_shortcert_run(tree_mode=True)
        agent_tree_ablation_run()
        print_summary_table(results)


if __name__ == "__main__":
    main()
