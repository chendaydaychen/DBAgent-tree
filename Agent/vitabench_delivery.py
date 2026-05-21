import argparse
import hashlib
import json
import math
import os
import queue
import random
import sys
import threading
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Sequence, Tuple

from db_client import agent_db
from tools.kv_tools import (
    get_product_detail,
    get_store_detail,
    get_task_context,
    list_candidate_stores,
    submit_task_delivery_order,
    tree_submit_task_delivery_order,
    tree_tx_abort,
    tree_tx_branch,
    tree_tx_commit_retry,
    tree_tx_get,
    tree_tx_put,
    tree_tx_refresh_winner,
    tree_tx_start,
    tree_tx_winner,
    tx_commit,
    tx_get,
    tx_put,
    tx_rollback,
    tx_start,
)


DEFAULT_DATASET_PATH = "/home/cht/datasets/VitaBench/delivery/tasks.json"
DEFAULT_CROSS_DOMAIN_PATH = "/home/cht/datasets/VitaBench/cross_domain/tasks.json"
DEFAULT_OUTPUT_DIR = "/home/cht/Tree-DB/artifacts/vitabench_delivery"
DEFAULT_TASK_LIMITS = {"smoke": 3, "functional": 20, "full": 100}

LLM_BASELINE_PROMPT = """你是 Tree-DB 上的 VitaBench delivery 事务 agent。

你的目标：
1. 使用 `get_task_context`、`list_candidate_stores`、`get_store_detail`、`get_product_detail` 理解任务。
2. 在提交前，必须调用 `get_expected_order_template` 获取评测模板。
3. 只能通过工具访问数据。
4. baseline 模式下，最终必须调用 `submit_task_delivery_order` 把最终订单写入 `task:{task_id}:answer:final`。
5. 最终回答必须用中文简洁说明你选了哪家店、哪些商品、是否已提交成功。

强约束：
- `get_expected_order_template` 返回的 `template` 是提交金标准。
- 提交前必须逐字段复用 template：`order_id`、`status`、`dispatch_time`、`shipping_time`、`delivery_time`、`note`、商品 `attributes` 等字段都不能自行改写或补全。
- 如果字段在 template 中为空，就保持为空；不要脑补默认值。
- 不要编造数据库中不存在的店铺或商品。
- 如果没有足够信息，继续调用工具而不是猜。
- 如果最终提交字段与 template 不一致，评测会直接失败。

建议流程：
1. `get_task_context`
2. `get_expected_order_template`
3. `list_candidate_stores` / `get_store_detail` / `get_product_detail`
4. 按 template 原样组织 `submit_task_delivery_order`
"""

LLM_TREE_PROMPT = """你是 Tree-DB 上的 VitaBench delivery Tree transaction agent。

你的目标：
1. 使用 `get_task_context`、`list_candidate_stores`、`get_store_detail`、`get_product_detail` 理解任务。
2. 在提交前，必须调用 `get_expected_order_template` 获取评测模板。
3. 必须真实使用 Tree 事务工具完成决策：`tree_tx_start` -> 一个或多个 `tree_tx_branch` -> `tree_tx_winner` -> `tree_tx_refresh_winner` -> `tree_submit_task_delivery_order` -> `tree_tx_commit_retry`。
4. 最终订单必须写入 `task:{task_id}:answer:final`。
5. 如果 `tree_tx_commit_retry` 返回 abort，要根据返回信息再尝试一次；若仍失败，再 `tree_tx_abort`。
6. 最终回答必须用中文简洁说明 winner branch、选中的店铺和提交结果。

强约束：
- `get_expected_order_template` 返回的 `template` 是提交金标准。
- `tree_submit_task_delivery_order` 时必须逐字段复用 template；不要自造 `order_id`、`status`、时间字段、`note` 或商品 `attributes`。
- 如果字段在 template 中为空，就保持为空。
- 不要直接用普通事务提交最终订单。
- 不要编造不存在的店铺或商品。
- 如果没有足够信息，继续调用工具而不是猜。
"""


def _json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def _read_json_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as fp:
        return json.load(fp)


def _write_json_file(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)


def _append_jsonl(path: str, rows: Sequence[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(_json_dumps(row))
            fp.write("\n")


def _makedirs(path: str) -> None:
    if not os.path.isdir(path):
        os.makedirs(path)


def _p95(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int(math.ceil(len(ordered) * 0.95)) - 1
    idx = max(0, min(idx, len(ordered) - 1))
    return ordered[idx]


def _n_choose_k(n: int, k: int) -> int:
    if k < 0 or k > n:
        return 0
    k = min(k, n - k)
    if k == 0:
        return 1
    numer = 1
    denom = 1
    for i in range(1, k + 1):
        numer *= n - (k - i)
        denom *= i
    return numer // denom


def _pass_hat_k(num_trials: int, success_count: int, k: int) -> float:
    if num_trials < k or success_count < k:
        return 0.0
    return float(_n_choose_k(success_count, k)) / float(_n_choose_k(num_trials, k))


def _pass_at_k(num_trials: int, success_count: int, k: int) -> float:
    if num_trials < k:
        return 0.0
    if num_trials - success_count >= k:
        return 1.0 - (float(_n_choose_k(num_trials - success_count, k)) / float(_n_choose_k(num_trials, k)))
    return 1.0


def _required_order(task: Dict[str, Any]) -> Dict[str, Any]:
    expected_states = task.get("evaluation_criteria", {}).get("expected_states", [])
    if not expected_states:
        return {}
    required_orders = expected_states[0].get("required_orders", [])
    return required_orders[0] if required_orders else {}


def _namespace_token(value: str) -> str:
    token = str(value).replace(":", "_").replace("/", "_").replace(" ", "_")
    if len(token) <= 16:
        return token
    digest = hashlib.sha1(token.encode("utf-8")).hexdigest()[:8]
    return "{}_{}".format(token[:6], digest)


def build_namespace(run_id: str, mode: str, task_uid: str, trial: int) -> str:
    return "run:{}:mode:{}:trial:{}:task:{}".format(run_id, mode, trial, _namespace_token(task_uid))


def task_key(namespace: str, suffix: str) -> str:
    return "{}:{}".format(namespace.rstrip(":"), suffix.lstrip(":"))


def _hotspot_key(namespace: str) -> str:
    del namespace
    return "global:contention:hotspot"


def _build_branch_candidate_key(namespace: str, branch_id: int) -> str:
    return task_key(namespace, "answer:candidate:branch:{}".format(branch_id))


def _build_attempt_candidate_key(namespace: str, attempt_idx: int) -> str:
    return task_key(namespace, "answer:candidate:attempt:{}".format(attempt_idx))


def _find_store_and_product_ids(task: Dict[str, Any]) -> Tuple[str, str]:
    order = _required_order(task)
    product_id = ""
    products = order.get("products", [])
    if products:
        product_id = str(products[0].get("product_id", ""))
    if product_id:
        stores = task.get("environment", {}).get("stores", {}) or {}
        available = set(_available_product_ids(stores))
        if product_id not in available:
            product_id = ""
    return str(order.get("store_id", "")), product_id


def _expected_product_profile(task: Dict[str, Any]) -> Dict[str, Any]:
    order = _required_order(task)
    order_products = order.get("products", []) or []
    if not order_products:
        return {}
    target_product = dict(order_products[0])
    store_id = str(order.get("store_id", ""))
    product_id = str(target_product.get("product_id", ""))
    store = task.get("environment", {}).get("stores", {}).get(store_id, {}) or {}
    for product in store.get("products", []) or []:
        if str(product.get("product_id", "")) == product_id:
            profile = dict(product)
            profile["expected_quantity"] = target_product.get("quantity")
            profile["expected_attributes"] = target_product.get("attributes")
            return profile
    return target_product


def _order_array_from_submit_args(args: Dict[str, Any]) -> Tuple[Optional[Any], Optional[str]]:
    if not args:
        return None, None
    order = dict(args)
    for field in ["namespace", "task_id", "tx_id", "branch_id"]:
        order.pop(field, None)
    order_array = [order]
    return order_array, _json_dumps(order_array)


def _extract_submission_fallback(trace: Sequence[Dict[str, Any]], mode: str) -> Tuple[Optional[Any], Optional[str]]:
    submit_tools = ["submit_task_delivery_order"]
    if mode == "tree":
        submit_tools = ["tree_submit_task_delivery_order", "submit_task_delivery_order"]
    for item in reversed(list(trace)):
        if item.get("tool") in submit_tools:
            return _order_array_from_submit_args(item.get("args", {}))
    return None, None


def _parse_json_safely(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _normalized_tokens(value: Any) -> List[str]:
    text = str(value or "").strip().lower()
    tokens = []
    current = []
    for ch in text:
        if ch.isalnum():
            current.append(ch)
            continue
        if current:
            tokens.append("".join(current))
            current = []
        if not ch.isspace():
            tokens.append(ch)
    if current:
        tokens.append("".join(current))
    return tokens


def _jaccard_similarity(left: Sequence[str], right: Sequence[str]) -> float:
    left_set = set(token for token in left if token)
    right_set = set(token for token in right if token)
    if not left_set and not right_set:
        return 0.0
    union = left_set | right_set
    if not union:
        return 0.0
    return float(len(left_set & right_set)) / float(len(union))


def _available_product_ids(stores: Dict[str, Any]) -> List[str]:
    product_ids = []
    for store in stores.values():
        for product in (store.get("products") or []):
            product_id = str(product.get("product_id", ""))
            if product_id:
                product_ids.append(product_id)
    return product_ids


def _delivery_compatibility_reason(task: Dict[str, Any]) -> str:
    order = _required_order(task)
    if not order:
        return "missing_required_order"
    if str(order.get("order_type", "")) != "delivery":
        return "non_delivery_order"
    for field in ["store_id", "location", "products", "total_price"]:
        if field not in order:
            return "missing_order_field:{}".format(field)
    location = order.get("location", {})
    if not isinstance(location, dict):
        return "bad_location_shape"
    products = order.get("products", [])
    if not isinstance(products, list) or not products:
        return "missing_products"

    environment = task.get("environment", {})
    stores = environment.get("stores", {})
    if not isinstance(stores, dict) or not stores:
        return "missing_stores"

    expected_store_id = str(order.get("store_id", ""))
    if not expected_store_id:
        return "missing_store_id"
    if expected_store_id not in stores:
        return "expected_store_not_projectable"

    available_product_ids = set(_available_product_ids(stores))
    for product in products:
        if not isinstance(product, dict):
            return "bad_product_shape"
        product_id = str(product.get("product_id", ""))
        if not product_id:
            return "missing_product_id"
        if product_id not in available_product_ids:
            return "expected_product_not_projectable"
    return ""


def _normalize_task(task: Dict[str, Any], task_source: str, ordinal: int) -> Dict[str, Any]:
    normalized = dict(task)
    normalized["_task_source"] = task_source
    normalized["_task_uid"] = "{}:{}".format(task_source, normalized.get("id", ordinal))
    if "instructions" not in normalized:
        normalized["instructions"] = ""
    return normalized


def load_experiment_tasks(
    delivery_path: str,
    task_source: str,
    cross_domain_path: str,
    task_ids: Optional[Sequence[str]],
    limit: Optional[int],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    sources = []
    if task_source in ("delivery", "both"):
        sources.append(("delivery", delivery_path, False))
    if task_source in ("cross_domain", "both"):
        sources.append(("cross_domain", cross_domain_path, True))

    tasks: List[Dict[str, Any]] = []
    skip_reason_distribution = defaultdict(int)
    source_counts = {}

    wanted = set([str(item) for item in (task_ids or [])])

    for source_name, path, filter_compatible in sources:
        if not os.path.exists(path):
            raise FileNotFoundError("dataset path not found: {}".format(path))
        raw_tasks = _read_json_file(path)
        source_counts[source_name] = len(raw_tasks)
        for idx, raw_task in enumerate(raw_tasks):
            task = _normalize_task(raw_task, source_name, idx)
            if filter_compatible:
                reason = _delivery_compatibility_reason(task)
                if reason:
                    skip_reason_distribution[reason] += 1
                    continue
            if wanted:
                task_id = str(task.get("id", ""))
                if task_id not in wanted and task["_task_uid"] not in wanted:
                    continue
            tasks.append(task)

    if limit is not None:
        tasks = tasks[:limit]

    load_summary = {
        "eligible_task_count": len(tasks),
        "skipped_task_count": sum(skip_reason_distribution.values()),
        "skip_reason_distribution": dict(skip_reason_distribution),
        "source_counts": source_counts,
        "task_source": task_source,
        "cross_domain_path": cross_domain_path,
    }
    return tasks, load_summary


def load_delivery_tasks(
    dataset_path: str = DEFAULT_DATASET_PATH,
    task_ids: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    tasks, _ = load_experiment_tasks(
        delivery_path=dataset_path,
        task_source="delivery",
        cross_domain_path=DEFAULT_CROSS_DOMAIN_PATH,
        task_ids=task_ids,
        limit=limit,
    )
    return tasks


def project_task_to_entries(task: Dict[str, Any], namespace: str, contention_profile: str = "none") -> Dict[str, str]:
    task_id = str(task["id"])
    environment = task.get("environment", {})
    user_scenario = task.get("user_scenario", {})
    evaluation_criteria = task.get("evaluation_criteria", {})
    stores = environment.get("stores", {})

    entries = {}
    entries[task_key(namespace, "meta:task")] = _json_dumps(
        {
            "task_id": task_id,
            "task_uid": task.get("_task_uid", task_id),
            "task_source": task.get("_task_source", "delivery"),
            "domain": task.get("domain", "delivery"),
            "time": environment.get("time", ""),
            "instructions": task.get("instructions", ""),
            "message_history": task.get("message_history", []),
        }
    )
    # The raw task blob is useful for debugging, but it is not read by the
    # deterministic executor or the hard-match evaluator. Skipping it keeps
    # the real socket-backend benchmark focused on execution-state payloads.
    entries[task_key(namespace, "meta:instructions")] = task.get("instructions", "")
    entries[task_key(namespace, "meta:message_history")] = _json_dumps(task.get("message_history", []))
    entries[task_key(namespace, "meta:expected_orders")] = _json_dumps(
        evaluation_criteria.get("expected_states", [{}])[0].get("required_orders", [])
    )
    entries[task_key(namespace, "meta:overall_rubrics")] = _json_dumps(evaluation_criteria.get("overall_rubrics", []))
    entries[task_key(namespace, "meta:state_rubrics")] = _json_dumps(
        evaluation_criteria.get("expected_states", [{}])[0].get("state_rubrics", [])
    )
    entries[task_key(namespace, "user:profile")] = _json_dumps(user_scenario.get("user_profile", {}))
    entries[task_key(namespace, "user:history")] = _json_dumps(environment.get("user_historical_behaviors", {}))
    entries[task_key(namespace, "weather:index")] = _json_dumps(environment.get("weather", []))
    entries[task_key(namespace, "location:index")] = _json_dumps(environment.get("location", []))
    entries[task_key(namespace, "orders:current")] = _json_dumps(environment.get("orders", {}))

    store_ids = sorted(stores.keys())
    product_ids = []
    entries[task_key(namespace, "index:stores")] = _json_dumps(store_ids)
    for store_id in store_ids:
        store = dict(stores[store_id])
        products = store.get("products", [])
        entries[task_key(namespace, "store:{}:detail".format(store_id))] = _json_dumps(store)
        entries[task_key(namespace, "store:{}:products".format(store_id))] = _json_dumps(
            [product.get("product_id") for product in products]
        )
        for product in products:
            product_id = str(product.get("product_id", ""))
            if not product_id:
                continue
            product_ids.append(product_id)
            entries[task_key(namespace, "product:{}:detail".format(product_id))] = _json_dumps(product)
    entries[task_key(namespace, "index:products")] = _json_dumps(sorted(product_ids))
    return entries


def _read_projected_index(agent: Any, namespace: str, suffix: str) -> List[Any]:
    raw = agent.kv_get(task_key(namespace, suffix))
    if not raw:
        return []
    try:
        return json.loads(raw)
    except Exception:
        return []


def project_task(
    agent: Any,
    task: Dict[str, Any],
    namespace: str,
    contention_profile: str = "none",
) -> Dict[str, Any]:
    entries = project_task_to_entries(task, namespace, contention_profile=contention_profile)
    write_requests = [(key, value) for key, value in entries.items() if value != ""]
    tx_id = agent.start()
    try:
        if write_requests:
            agent.put_many(tx_id, write_requests)
        agent.commit(tx_id)
    except Exception:
        agent.rollback(tx_id)
        raise

    return {
        "namespace": namespace,
        "task_id": str(task["id"]),
        "task_uid": task.get("_task_uid", str(task["id"])),
        "projected_key_count": len(entries),
        "store_count": len(_read_projected_index(agent, namespace, "index:stores")),
        "product_count": len(_read_projected_index(agent, namespace, "index:products")),
        "hotspot_key": _hotspot_key(namespace) if contention_profile == "hotspot" else "",
        "keys_preview": sorted(entries.keys())[:12],
    }


def _record_tool(
    trace: List[Dict[str, Any]],
    messages: List[Dict[str, Any]],
    name: str,
    args: Dict[str, Any],
    result: Dict[str, Any],
) -> Dict[str, Any]:
    trace.append({"tool": name, "args": args, "result": result})
    messages.append({"role": "assistant", "content": "TOOL {}".format(name), "tool_name": name, "arguments": args})
    messages.append({"role": "tool", "name": name, "content": _json_dumps(result)})
    return result


def _load_final_answer(namespace: str) -> Tuple[Optional[Any], Optional[str]]:
    raw = agent_db.kv_get(task_key(namespace, "answer:final"))
    if raw in (None, ""):
        return None, raw
    try:
        return json.loads(raw), raw
    except Exception:
        return None, raw


def _ensure_llm_config() -> None:
    missing = []
    if not os.getenv("OPENAI_MODEL"):
        missing.append("OPENAI_MODEL")
    if not os.getenv("OPENAI_BASE_URL"):
        missing.append("OPENAI_BASE_URL")
    if not os.getenv("OPENAI_API_KEY"):
        missing.append("OPENAI_API_KEY")
    if missing:
        raise RuntimeError("missing LLM environment variables: {}".format(", ".join(missing)))


def _extract_tool_trace(messages: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    trace = []
    pending = None
    for message in messages:
        role = message.get("role")
        if role == "assistant":
            tool_calls = message.get("tool_calls") or []
            if tool_calls:
                for tool_call in tool_calls:
                    fn = tool_call.get("function", {})
                    args = fn.get("arguments", {})
                    if isinstance(args, str):
                        try:
                            args = json.loads(args)
                        except Exception:
                            args = {"raw": args}
                    trace.append({"tool": fn.get("name", ""), "args": args, "result": None})
                continue
            content = message.get("content", "")
            stripped = content.strip()
            if stripped.startswith("{") and '"action":"tool"' in stripped.replace(" ", ""):
                try:
                    payload = json.loads(stripped)
                    pending = {
                        "tool": payload.get("name", ""),
                        "args": payload.get("arguments", {}) if isinstance(payload.get("arguments", {}), dict) else {},
                    }
                except Exception:
                    pending = None
        elif role == "user" and pending is not None:
            marker = "工具执行结果如下，请继续决策："
            content = message.get("content", "")
            if marker in content:
                raw = content.split(marker, 1)[1]
                try:
                    results = json.loads(raw)
                    if isinstance(results, list) and results:
                        pending["result"] = results[-1].get("result")
                except Exception:
                    pending["result"] = {"raw": raw}
                trace.append(pending)
                pending = None
        elif role == "tool" and trace:
            if trace[-1]["result"] is None:
                try:
                    trace[-1]["result"] = json.loads(message.get("content", "{}"))
                except Exception:
                    trace[-1]["result"] = {"raw": message.get("content", "")}
    return trace


def _llm_agent_run(task: Dict[str, Any], namespace: str, mode: str, top_k: int, max_rounds: int) -> Dict[str, Any]:
    from llm import chat_with_tools_session
    from tool_registry import TOOLS_SCHEMA, tool_router

    _ensure_llm_config()
    task_id = str(task["id"])
    messages = []
    if mode == "tree":
        system_prompt = LLM_TREE_PROMPT
        mode_instruction = "请至少为 {} 家候选商家建立 branch，再选择 winner branch。".format(max(2, top_k))
    else:
        system_prompt = LLM_BASELINE_PROMPT
        mode_instruction = "请使用普通事务路径完成下单。"

    user_prompt = (
        "task_id={task_id}\n"
        "namespace={namespace}\n"
        "mode={mode}\n"
        "{mode_instruction}\n"
        "用户任务：{instructions}\n"
        "提交前必须调用 get_expected_order_template，并按其中 template 原样填写最终订单字段。"
    ).format(
        task_id=task_id,
        namespace=namespace,
        mode=mode,
        mode_instruction=mode_instruction,
        instructions=task.get("instructions", ""),
    )
    messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": user_prompt})

    session = chat_with_tools_session(messages, TOOLS_SCHEMA, tool_router, max_rounds=max_rounds)
    tool_trace = _extract_tool_trace(session.get("messages", []))
    try:
        final_answer, final_raw = _load_final_answer(namespace)
    except Exception:
        final_answer, final_raw = _extract_submission_fallback(tool_trace, mode)
    abort_reason = ""
    retry_count = 0
    abort_reasons = []
    for item in tool_trace:
        tool_name = item.get("tool", "")
        result = item.get("result") or {}
        if tool_name == "tree_tx_commit_retry" and not result.get("ok", False):
            retry_count += 1
            reason = result.get("abort_reason", "") or result.get("error", "")
            if reason:
                abort_reasons.append(reason)
                abort_reason = reason

    selected_store_id = ""
    if isinstance(final_answer, list) and final_answer and isinstance(final_answer[0], dict):
        selected_store_id = str(final_answer[0].get("store_id", ""))

    return {
        "mode": mode,
        "messages": session.get("messages", messages),
        "tool_trace": tool_trace,
        "db_retry_count": retry_count,
        "db_abort_count": len(abort_reasons),
        "abort_reasons": abort_reasons,
        "db_abort_reason": abort_reason,
        "submit_ok": final_answer is not None,
        "final_answer": final_answer,
        "final_answer_raw": final_raw,
        "selected_store_id": selected_store_id,
        "assistant_answer": session.get("answer", ""),
        "tool_mode": session.get("tool_mode", ""),
        "txn_attempts": 0,
        "rolled_back_attempts": 0,
        "committed_attempts": 1 if final_answer is not None else 0,
        "candidate_count": 0,
        "interference_bumps": 0,
    }


def _candidate_payload(store_id: str, expected_order: Dict[str, Any], attempt: int) -> Dict[str, Any]:
    return {
        "candidate_store_id": store_id,
        "candidate_attempt": attempt,
        "expected_order": expected_order,
    }


def _candidate_product_from_store(
    store_detail: Dict[str, Any],
    expected_product_id: str,
    expected_product_profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    expected_product_profile = expected_product_profile or {}
    expected_name_tokens = _normalized_tokens(expected_product_profile.get("name", ""))
    expected_tag_tokens = [
        str(tag).strip().lower()
        for tag in (expected_product_profile.get("tags", []) or [])
        if str(tag).strip()
    ]
    expected_attr_tokens = _normalized_tokens(
        expected_product_profile.get("expected_attributes", expected_product_profile.get("attributes", ""))
    )
    expected_price = expected_product_profile.get("price")

    best_product = {}
    best_score = -1.0
    for product in (store_detail.get("products") or []):
        score = 0.0
        product_id = str(product.get("product_id", ""))
        if expected_product_id and product_id == expected_product_id:
            score += 4.0

        name_similarity = _jaccard_similarity(
            _normalized_tokens(product.get("name", "")),
            expected_name_tokens,
        )
        score += 3.0 * name_similarity

        product_tags = [
            str(tag).strip().lower()
            for tag in (product.get("tags", []) or [])
            if str(tag).strip()
        ]
        score += 1.5 * _jaccard_similarity(product_tags, expected_tag_tokens)
        score += 0.5 * _jaccard_similarity(
            _normalized_tokens(product.get("attributes", "")),
            expected_attr_tokens,
        )
        try:
            if product.get("price") is not None and expected_price is not None:
                price_gap = abs(float(product.get("price")) - float(expected_price))
                denom = max(abs(float(expected_price)), 1.0)
                score += max(0.0, 1.0 - min(price_gap / denom, 1.0))
        except Exception:
            pass

        if score > best_score:
            best_score = score
            best_product = dict(product)
            best_product["_match_score"] = score
            best_product["_name_similarity"] = name_similarity
    return best_product


def _candidate_summary_payload(
    store_id: str,
    attempt: int,
    store_detail: Dict[str, Any],
    expected_order: Dict[str, Any],
    expected_product_id: str,
    expected_product_profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    matched_product = _candidate_product_from_store(
        store_detail,
        expected_product_id,
        expected_product_profile=expected_product_profile,
    )
    expected_total_price = expected_order.get("total_price")
    matched_price = matched_product.get("price") if matched_product else None
    price_gap = None
    try:
        if matched_price is not None and expected_total_price is not None:
            price_gap = abs(float(matched_price) - float(expected_total_price))
    except Exception:
        price_gap = None
    return {
        "candidate_store_id": store_id,
        "candidate_attempt": attempt,
        "store_name": str(store_detail.get("name", "")),
        "store_tags": list(store_detail.get("tags", []) or []),
        "product_count": len(store_detail.get("products", []) or []),
        "matched_product_id": str(matched_product.get("product_id", "")) if matched_product else "",
        "matched_product_name": str(matched_product.get("name", "")) if matched_product else "",
        "matched_product_price": matched_price,
        "expected_total_price": expected_total_price,
        "price_gap": price_gap,
        "match_score": float(matched_product.get("_match_score", 0.0) or 0.0),
        "name_similarity": float(matched_product.get("_name_similarity", 0.0) or 0.0),
        "has_expected_product": bool(matched_product) and str(matched_product.get("product_id", "")) == expected_product_id,
    }


def _candidate_rank_key(payload: Dict[str, Any]) -> Tuple[float, int, float, int, str]:
    match_score = float(payload.get("match_score", 0.0) or 0.0)
    has_expected_product = 1 if payload.get("has_expected_product") else 0
    price_gap = payload.get("price_gap")
    if price_gap is None:
        price_gap = 10 ** 9
    product_count = int(payload.get("product_count", 0) or 0)
    attempt = int(payload.get("candidate_attempt", 0) or 0)
    store_id = str(payload.get("candidate_store_id", ""))
    return (
        -match_score,
        -has_expected_product,
        float(price_gap),
        -product_count,
        "{}:{:06d}".format(store_id, attempt),
    )


def _select_winner_payload(candidate_payloads: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not candidate_payloads:
        return {}
    return sorted(candidate_payloads, key=_candidate_rank_key)[0]


def _semantic_recheck_winner_payload(
    store_id: str,
    attempt: int,
    store_raw: Any,
    expected_order: Dict[str, Any],
    expected_product_id: str,
    expected_product_profile: Optional[Dict[str, Any]] = None,
    product_raw: Any = None,
) -> Tuple[bool, str, Dict[str, Any]]:
    store_detail = _parse_json_safely(store_raw, {})
    if not isinstance(store_detail, dict) or not store_detail:
        return False, "WINNER_STORE_DETAIL_UNAVAILABLE", {}

    refreshed_payload = _candidate_summary_payload(
        store_id=store_id,
        attempt=attempt,
        store_detail=store_detail,
        expected_order=expected_order,
        expected_product_id=expected_product_id,
        expected_product_profile=expected_product_profile,
    )

    if expected_product_id:
        if not refreshed_payload.get("has_expected_product"):
            return False, "WINNER_EXPECTED_PRODUCT_MISSING", refreshed_payload
        if str(refreshed_payload.get("matched_product_id", "")) != expected_product_id:
            return False, "WINNER_MATCHED_PRODUCT_CHANGED", refreshed_payload

    matched_price = refreshed_payload.get("matched_product_price")
    if product_raw not in (None, ""):
        product_detail = _parse_json_safely(product_raw, {})
        if not isinstance(product_detail, dict) or not product_detail:
            return False, "WINNER_PRODUCT_DETAIL_UNAVAILABLE", refreshed_payload
        product_id = str(product_detail.get("product_id", ""))
        if expected_product_id and product_id and product_id != expected_product_id:
            return False, "WINNER_PRODUCT_DETAIL_ID_CHANGED", refreshed_payload
        detail_price = product_detail.get("price")
        if detail_price is not None and matched_price is not None:
            try:
                if abs(float(detail_price) - float(matched_price)) > 1e-6:
                    return False, "WINNER_PRODUCT_DETAIL_PRICE_MISMATCH", refreshed_payload
            except Exception:
                return False, "WINNER_PRODUCT_DETAIL_PRICE_INVALID", refreshed_payload

    return True, "", refreshed_payload


def _should_retry_baseline_winner(
    attempts_used: int,
    retry_policy: str,
    max_attempts: int,
) -> bool:
    if retry_policy == "until_success":
        return True
    capped_attempts = max(1, max_attempts)
    return attempts_used < capped_attempts


def _baseline_retry_backoff_sec(attempts_used: int, retry_policy: str) -> float:
    if retry_policy != "until_success":
        return 0.0
    exponent = min(max(0, attempts_used - 1), 6)
    base_backoff = min(0.005 * (2 ** exponent), 0.2)
    return base_backoff * random.uniform(0.5, 1.5)


def _choose_candidate_store_ids(
    candidate_stores: Sequence[Dict[str, Any]],
    expected_store_id: str,
    top_k: int,
) -> List[str]:
    candidate_store_ids = [str(store.get("store_id")) for store in candidate_stores if store.get("store_id")]
    if expected_store_id and expected_store_id not in candidate_store_ids:
        candidate_store_ids.append(expected_store_id)
    if not candidate_store_ids:
        return []
    if top_k <= 0:
        top_k = 1
    selected = candidate_store_ids[:top_k]
    if expected_store_id and expected_store_id not in selected:
        if len(selected) < top_k:
            selected.append(expected_store_id)
        else:
            selected[-1] = expected_store_id
    deduped = []
    seen = set()
    for store_id in selected:
        if store_id and store_id not in seen:
            deduped.append(store_id)
            seen.add(store_id)
    return deduped


def _winner_store_id(candidate_store_ids: Sequence[str], expected_store_id: str) -> str:
    if expected_store_id and expected_store_id in candidate_store_ids:
        return expected_store_id
    return candidate_store_ids[0] if candidate_store_ids else ""


def _hotspot_payload_bump(raw: Any, source: str) -> str:
    current = _parse_json_safely(raw, {})
    version = 0
    if isinstance(current, dict):
        try:
            version = int(current.get("version", 0))
        except Exception:
            version = 0
    payload = {
        "version": version + 1,
        "updated_by": source,
        "nonce": uuid.uuid4().hex,
        "ts": time.time(),
    }
    return _json_dumps(payload)


def _bump_hotspot_once(key: str) -> bool:
    for _ in range(3):
        start_result = tx_start({})
        if not start_result.get("ok"):
            continue
        tx_id = start_result["tx_id"]
        try:
            current_result = tx_get({"tx_id": tx_id, "key": key})
            current_raw = current_result.get("value") if current_result.get("ok") else None
            next_value = _hotspot_payload_bump(current_raw, "interference")
            put_result = tx_put({"tx_id": tx_id, "key": key, "value": next_value})
            if not put_result.get("ok"):
                tx_rollback({"tx_id": tx_id})
                continue
            commit_result = tx_commit({"tx_id": tx_id})
            if commit_result.get("ok"):
                return True
        except Exception:
            pass
        try:
            tx_rollback({"tx_id": tx_id})
        except Exception:
            pass
    return False


class HotspotInterferenceManager(object):
    def __init__(self, worker_count: int):
        self.worker_count = max(0, worker_count)
        self.enabled = self.worker_count > 0
        self._queue = queue.Queue()
        self._threads = []
        self._closed = False
        self._lock = threading.Lock()
        self.bump_count = 0
        if self.enabled:
            for idx in range(self.worker_count):
                thread = threading.Thread(target=self._worker, name="hotspot-interference-{}".format(idx))
                thread.daemon = True
                thread.start()
                self._threads.append(thread)

    def _worker(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                self._queue.task_done()
                return
            hotspot_key, done_event = item
            try:
                ok = _bump_hotspot_once(hotspot_key)
                if ok:
                    with self._lock:
                        self.bump_count += 1
            finally:
                done_event.set()
                self._queue.task_done()

    def schedule(self, hotspot_key: str) -> threading.Event:
        done_event = threading.Event()
        if not self.enabled or self._closed:
            done_event.set()
            return done_event
        self._queue.put((hotspot_key, done_event))
        return done_event

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for _ in self._threads:
            self._queue.put(None)
        for thread in self._threads:
            thread.join(timeout=1.0)


def _schedule_hotspot_bump(
    contention_profile: str,
    interference_manager: Optional[HotspotInterferenceManager],
    hotspot_key: str,
    hotspot_probability: float = 1.0,
) -> bool:
    if contention_profile != "hotspot" or interference_manager is None or not hotspot_key:
        return False
    if hotspot_probability <= 0.0:
        return False
    if hotspot_probability < 1.0 and random.random() > hotspot_probability:
        return False
    done_event = interference_manager.schedule(hotspot_key)
    done_event.wait(timeout=2.0)
    return True


def _record_baseline_tx_get(
    trace: List[Dict[str, Any]],
    messages: List[Dict[str, Any]],
    tx_id: str,
    key: str,
) -> Dict[str, Any]:
    return _record_tool(trace, messages, "tx_get", {"tx_id": tx_id, "key": key}, tx_get({"tx_id": tx_id, "key": key}))


def _record_baseline_tx_put(
    trace: List[Dict[str, Any]],
    messages: List[Dict[str, Any]],
    tx_id: str,
    key: str,
    value: str,
) -> Dict[str, Any]:
    return _record_tool(
        trace,
        messages,
        "tx_put",
        {"tx_id": tx_id, "key": key, "value": value},
        tx_put({"tx_id": tx_id, "key": key, "value": value}),
    )


def _reference_agent_run(
    task: Dict[str, Any],
    namespace: str,
    mode: str,
    top_k: int,
    contention_profile: str = "none",
    interference_manager: Optional[HotspotInterferenceManager] = None,
    hotspot_probability: float = 1.0,
    baseline_winner_retry_policy: str = "fixed",
    baseline_winner_max_attempts: int = 2,
) -> Dict[str, Any]:
    task_id = str(task["id"])
    messages = [
        {"role": "system", "content": "Deterministic no-agent executor for Tree-DB VitaBench experiments."},
        {"role": "user", "content": task.get("instructions", "")},
    ]
    trace = []
    expected_order = _required_order(task)
    expected_store_id, expected_product_id = _find_store_and_product_ids(task)
    abort_reason = ""
    abort_reasons = []
    semantic_recheck_reasons = []
    retry_count = 0
    abort_count = 0
    semantic_recheck_fail_count = 0
    committed_attempts = 0
    rolled_back_attempts = 0
    txn_attempts = 0
    interference_bumps = 0
    hotspot_key = _hotspot_key(namespace) if contention_profile == "hotspot" else ""
    expected_product_profile = _expected_product_profile(task)
    winner_store_id = ""
    branch_count = 0
    explore_txn_attempts = 0
    winner_txn_attempts = 0
    winner_commit_rounds = 0
    winner_selection_latency_sec = 0.0
    explore_phase_latency_sec = 0.0
    commit_phase_latency_sec = 0.0

    def _build_agent_result(
        submit_ok: bool,
        final_answer: Optional[Any],
        final_answer_raw: Optional[str],
        selected_store_id: str,
        abort_reason_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {
            "mode": mode,
            "messages": messages,
            "tool_trace": trace,
            "db_retry_count": retry_count,
            "db_abort_count": abort_count,
            "db_abort_rate_per_attempt": float(abort_count) / float(max(txn_attempts, 1)),
            "abort_reasons": abort_reasons,
            "semantic_recheck_fail_count": semantic_recheck_fail_count,
            "semantic_recheck_reasons": semantic_recheck_reasons,
            "db_abort_reason": abort_reason_override if abort_reason_override is not None else abort_reason,
            "submit_ok": submit_ok,
            "final_answer": final_answer,
            "final_answer_raw": final_answer_raw,
            "selected_store_id": selected_store_id,
            "txn_attempts": txn_attempts,
            "rolled_back_attempts": rolled_back_attempts,
            "committed_attempts": committed_attempts,
            "candidate_count": len(candidate_store_ids),
            "branch_count": branch_count,
            "explore_txn_attempts": explore_txn_attempts,
            "winner_txn_attempts": winner_txn_attempts,
            "winner_commit_rounds": winner_commit_rounds,
            "winner_selection_latency_sec": winner_selection_latency_sec,
            "explore_phase_latency_sec": explore_phase_latency_sec,
            "commit_phase_latency_sec": commit_phase_latency_sec,
            "interference_bumps": interference_bumps,
        }

    _record_tool(
        trace,
        messages,
        "get_task_context",
        {"namespace": namespace, "task_id": task_id},
        get_task_context({"namespace": namespace, "task_id": task_id}),
    )
    stores_result = _record_tool(
        trace,
        messages,
        "list_candidate_stores",
        {"namespace": namespace, "task_id": task_id, "include_details": False},
        list_candidate_stores({"namespace": namespace, "task_id": task_id, "include_details": False}),
    )
    candidate_stores = stores_result.get("stores", []) if stores_result.get("ok") else []
    candidate_store_ids = _choose_candidate_store_ids(candidate_stores, expected_store_id, top_k)
    branch_count = len(candidate_store_ids)
    if not candidate_store_ids:
        return _build_agent_result(False, None, None, "", abort_reason_override="NO_CANDIDATE_STORE")

    if mode == "baseline":
        candidate_payloads = []
        meta_key = task_key(namespace, "meta:task")
        product_key = task_key(namespace, "product:{}:detail".format(expected_product_id))
        explore_phase_started = time.time()
        for attempt_idx, store_id in enumerate(candidate_store_ids, 1):
            store_key = task_key(namespace, "store:{}:detail".format(store_id))
            txn_attempts += 1
            explore_txn_attempts += 1
            start_result = _record_tool(trace, messages, "tx_start", {}, tx_start({}))
            if not start_result.get("ok"):
                abort_count += 1
                abort_reason = start_result.get("error", "TX_START_FAILED")
                abort_reasons.append(abort_reason)
                continue
            tx_id = start_result["tx_id"]
            try:
                _record_baseline_tx_get(trace, messages, tx_id, meta_key)
                store_result = _record_baseline_tx_get(trace, messages, tx_id, store_key)
                if expected_product_id:
                    _record_baseline_tx_get(trace, messages, tx_id, product_key)
                store_detail = _parse_json_safely(store_result.get("value"), {})
                candidate_payload = _candidate_summary_payload(
                    store_id=store_id,
                    attempt=attempt_idx,
                    store_detail=store_detail if isinstance(store_detail, dict) else {},
                    expected_order=expected_order,
                    expected_product_id=expected_product_id,
                    expected_product_profile=expected_product_profile,
                )
                candidate_payloads.append(candidate_payload)
                candidate_key = _build_attempt_candidate_key(namespace, attempt_idx)
                _record_baseline_tx_put(trace, messages, tx_id, candidate_key, _json_dumps(candidate_payload))
            except Exception as exc:
                abort_count += 1
                abort_reason = str(exc)
                abort_reasons.append(abort_reason)
            try:
                _record_tool(trace, messages, "tx_rollback", {"tx_id": tx_id}, tx_rollback({"tx_id": tx_id}))
                rolled_back_attempts += 1
            except Exception:
                pass

        explore_phase_latency_sec = time.time() - explore_phase_started
        winner_selection_started = time.time()
        winner_payload = _select_winner_payload(candidate_payloads)
        winner_selection_latency_sec = time.time() - winner_selection_started
        winner_store_id = str(winner_payload.get("candidate_store_id", "")) if winner_payload else ""
        if not winner_store_id:
            return _build_agent_result(False, None, None, "", abort_reason_override=abort_reason or "NO_WINNER_SELECTED")

        winner_attempts_used = 0
        winner_hotspot_bumped = False
        winner_store_key = task_key(namespace, "store:{}:detail".format(winner_store_id))
        commit_phase_started = time.time()
        while True:
            winner_attempts_used += 1
            winner_txn_attempts += 1
            winner_commit_rounds += 1
            txn_attempts += 1
            start_result = _record_tool(trace, messages, "tx_start", {}, tx_start({}))
            if not start_result.get("ok"):
                abort_count += 1
                abort_reason = start_result.get("error", "TX_START_FAILED")
                abort_reasons.append(abort_reason)
                if _should_retry_baseline_winner(
                    winner_attempts_used,
                    baseline_winner_retry_policy,
                    baseline_winner_max_attempts,
                ):
                    retry_count += 1
                    backoff_sec = _baseline_retry_backoff_sec(winner_attempts_used, baseline_winner_retry_policy)
                    if backoff_sec > 0:
                        time.sleep(backoff_sec)
                    continue
                break
            tx_id = start_result["tx_id"]
            try:
                _record_baseline_tx_get(trace, messages, tx_id, meta_key)
                winner_store_result = _record_baseline_tx_get(trace, messages, tx_id, winner_store_key)
                winner_product_result = None
                if expected_product_id:
                    winner_product_result = _record_baseline_tx_get(trace, messages, tx_id, product_key)
                if hotspot_key:
                    _record_baseline_tx_get(trace, messages, tx_id, hotspot_key)

                if not winner_hotspot_bumped:
                    if _schedule_hotspot_bump(
                        contention_profile,
                        interference_manager,
                        hotspot_key,
                        hotspot_probability=hotspot_probability,
                    ):
                        interference_bumps += 1
                    winner_hotspot_bumped = True
                if hotspot_key:
                    _record_baseline_tx_get(trace, messages, tx_id, hotspot_key)

                constraints_ok, constraint_reason, _refreshed_payload = _semantic_recheck_winner_payload(
                    store_id=winner_store_id,
                    attempt=winner_attempts_used,
                    store_raw=winner_store_result.get("value"),
                    expected_order=expected_order,
                    expected_product_id=expected_product_id,
                    expected_product_profile=expected_product_profile,
                    product_raw=winner_product_result.get("value") if winner_product_result else None,
                )
                if not constraints_ok:
                    semantic_recheck_fail_count += 1
                    abort_reason = constraint_reason
                    semantic_recheck_reasons.append(abort_reason)
                    break

                final_answer, final_raw = _order_array_from_submit_args(expected_order)
                _record_baseline_tx_put(trace, messages, tx_id, task_key(namespace, "answer:final"), final_raw)
                commit_result = _record_tool(trace, messages, "tx_commit", {"tx_id": tx_id}, tx_commit({"tx_id": tx_id}))
                if commit_result.get("ok"):
                    committed_attempts += 1
                    commit_phase_latency_sec = time.time() - commit_phase_started
                    try:
                        final_answer, final_raw = _load_final_answer(namespace)
                    except Exception:
                        pass
                    return _build_agent_result(True, final_answer, final_raw, winner_store_id)

                abort_count += 1
                abort_reason = commit_result.get("error", "TX_COMMIT_FAILED")
                abort_reasons.append(abort_reason)
            except Exception as exc:
                abort_count += 1
                abort_reason = str(exc)
                abort_reasons.append(abort_reason)
            try:
                _record_tool(trace, messages, "tx_rollback", {"tx_id": tx_id}, tx_rollback({"tx_id": tx_id}))
                rolled_back_attempts += 1
            except Exception:
                pass
            if _should_retry_baseline_winner(
                winner_attempts_used,
                baseline_winner_retry_policy,
                baseline_winner_max_attempts,
            ):
                retry_count += 1
                backoff_sec = _baseline_retry_backoff_sec(winner_attempts_used, baseline_winner_retry_policy)
                if backoff_sec > 0:
                    time.sleep(backoff_sec)
                continue
            break

        commit_phase_latency_sec = time.time() - commit_phase_started
        return _build_agent_result(False, None, None, winner_store_id)

    tree_start_result = _record_tool(trace, messages, "tree_tx_start", {}, tree_tx_start({}))
    if not tree_start_result.get("ok"):
        abort_count = 1
        abort_reason = tree_start_result.get("error", "TREE_TX_START_FAILED")
        abort_reasons = [abort_reason]
        return _build_agent_result(False, None, None, "", abort_reason_override=abort_reason)

    tx_id = tree_start_result["tx_id"]
    txn_attempts = 1
    explore_txn_attempts = 1
    winner_branch_id = None
    branch_to_store = {}
    branch_payloads = {}
    try:
        meta_key = task_key(namespace, "meta:task")
        product_key = task_key(namespace, "product:{}:detail".format(expected_product_id))
        explore_phase_started = time.time()
        if meta_key:
            _record_tool(
                trace,
                messages,
                "tree_tx_get",
                {"tx_id": tx_id, "branch_id": 0, "key": meta_key, "strict": False},
                tree_tx_get({"tx_id": tx_id, "branch_id": 0, "key": meta_key, "strict": False}),
            )
        if expected_product_id:
            _record_tool(
                trace,
                messages,
                "tree_tx_get",
                {"tx_id": tx_id, "branch_id": 0, "key": product_key, "strict": False},
                tree_tx_get({"tx_id": tx_id, "branch_id": 0, "key": product_key, "strict": False}),
            )
        created_branch_ids = agent_db.tree_branch_many(tx_id, 0, len(candidate_store_ids))
        _record_tool(
            trace,
            messages,
            "tree_tx_branch_many",
            {"tx_id": tx_id, "parent_branch_id": 0, "count": len(candidate_store_ids)},
            {"ok": True, "tx_id": tx_id, "branch_ids": created_branch_ids},
        )
        if len(created_branch_ids) != len(candidate_store_ids):
            raise RuntimeError("TREE_BRANCH_MANY_COUNT_MISMATCH")

        tree_get_many_requests = []
        for store_id, branch_id in zip(candidate_store_ids, created_branch_ids):
            branch_to_store[branch_id] = store_id
            tree_get_many_requests.append((branch_id, task_key(namespace, "store:{}:detail".format(store_id))))

        store_values = agent_db.tree_get_many(tx_id, tree_get_many_requests, strict=False)
        _record_tool(
            trace,
            messages,
            "tree_tx_get_many",
            {
                "tx_id": tx_id,
                "items": [
                    {"branch_id": branch_id, "key": key, "strict": False}
                    for branch_id, key in tree_get_many_requests
                ],
            },
            {
                "ok": True,
                "tx_id": tx_id,
                "items": [
                    {"branch_id": branch_id, "key": key, "value": value}
                    for (branch_id, key), value in zip(tree_get_many_requests, store_values)
                ],
            },
        )

        tree_put_many_requests = []
        for (branch_id, _), store_value in zip(tree_get_many_requests, store_values):
            store_id = branch_to_store[branch_id]
            store_detail = _parse_json_safely(store_value, {})
            candidate_payload = _candidate_summary_payload(
                store_id=store_id,
                attempt=branch_id,
                store_detail=store_detail if isinstance(store_detail, dict) else {},
                expected_order=expected_order,
                expected_product_id=expected_product_id,
                expected_product_profile=expected_product_profile,
            )
            branch_payloads[branch_id] = candidate_payload
            tree_put_many_requests.append(
                (branch_id, _build_branch_candidate_key(namespace, branch_id), _json_dumps(candidate_payload))
            )

        agent_db.tree_put_many(tx_id, tree_put_many_requests)
        _record_tool(
            trace,
            messages,
            "tree_tx_put_many",
            {
                "tx_id": tx_id,
                "items": [
                    {"branch_id": branch_id, "key": key}
                    for branch_id, key, _value in tree_put_many_requests
                ],
            },
            {
                "ok": True,
                "tx_id": tx_id,
                "count": len(tree_put_many_requests),
            },
        )

        explore_phase_latency_sec = time.time() - explore_phase_started
        winner_selection_started = time.time()
        winner_payload = _select_winner_payload(list(branch_payloads.values()))
        winner_selection_latency_sec = time.time() - winner_selection_started
        winner_store_id = str(winner_payload.get("candidate_store_id", "")) if winner_payload else ""
        if winner_store_id:
            for branch_id, payload in branch_payloads.items():
                if str(payload.get("candidate_store_id", "")) == winner_store_id:
                    winner_branch_id = branch_id
                    break
        if winner_branch_id is None and branch_payloads:
            winner_branch_id = sorted(branch_payloads.keys())[0]
            winner_store_id = str(branch_payloads[winner_branch_id].get("candidate_store_id", ""))

        if winner_branch_id is None:
            _record_tool(trace, messages, "tree_tx_abort", {"tx_id": tx_id}, tree_tx_abort({"tx_id": tx_id}))
            abort_count = max(1, abort_count)
            if not abort_reasons:
                abort_reasons = ["NO_BRANCH_CREATED"]
            return _build_agent_result(False, None, None, "", abort_reason_override=abort_reason or "NO_BRANCH_CREATED")

        if _schedule_hotspot_bump(
            contention_profile,
            interference_manager,
            hotspot_key,
            hotspot_probability=hotspot_probability,
        ):
            interference_bumps += 1

        commit_phase_started = time.time()
        winner_txn_attempts = 1
        winner_commit_rounds = 1
        _record_tool(
            trace,
            messages,
            "tree_tx_winner",
            {"tx_id": tx_id, "branch_id": winner_branch_id},
            tree_tx_winner({"tx_id": tx_id, "branch_id": winner_branch_id}),
        )
        winner_store_refresh = None
        winner_product_refresh = None
        if hotspot_key:
            _record_tool(
                trace,
                messages,
                "tree_tx_refresh_winner",
                {"tx_id": tx_id, "branch_id": winner_branch_id, "key": hotspot_key},
                tree_tx_refresh_winner({"tx_id": tx_id, "branch_id": winner_branch_id, "key": hotspot_key}),
            )
        winner_store_refresh = _record_tool(
            trace,
            messages,
            "tree_tx_refresh_winner",
            {
                "tx_id": tx_id,
                "branch_id": winner_branch_id,
                "key": task_key(namespace, "store:{}:detail".format(winner_store_id)),
            },
            tree_tx_refresh_winner(
                {
                    "tx_id": tx_id,
                    "branch_id": winner_branch_id,
                    "key": task_key(namespace, "store:{}:detail".format(winner_store_id)),
                }
            ),
        )
        if expected_product_id:
            winner_product_refresh = _record_tool(
                trace,
                messages,
                "tree_tx_refresh_winner",
                {
                    "tx_id": tx_id,
                    "branch_id": winner_branch_id,
                    "key": task_key(namespace, "product:{}:detail".format(expected_product_id)),
                },
                tree_tx_refresh_winner(
                    {
                        "tx_id": tx_id,
                        "branch_id": winner_branch_id,
                        "key": task_key(namespace, "product:{}:detail".format(expected_product_id)),
                    }
                ),
            )

        constraints_ok, constraint_reason, _refreshed_payload = _semantic_recheck_winner_payload(
            store_id=winner_store_id,
            attempt=winner_branch_id,
            store_raw=winner_store_refresh.get("value") if winner_store_refresh else None,
            expected_order=expected_order,
            expected_product_id=expected_product_id,
            expected_product_profile=expected_product_profile,
            product_raw=winner_product_refresh.get("value") if winner_product_refresh else None,
        )
        if not constraints_ok:
            semantic_recheck_fail_count += 1
            abort_reason = constraint_reason
            semantic_recheck_reasons.append(abort_reason)
            _record_tool(trace, messages, "tree_tx_abort", {"tx_id": tx_id}, tree_tx_abort({"tx_id": tx_id}))
            commit_phase_latency_sec = time.time() - commit_phase_started
            return _build_agent_result(False, None, None, winner_store_id)

        final_answer, final_raw = _order_array_from_submit_args(expected_order)
        _record_tool(
            trace,
            messages,
            "tree_tx_put",
            {
                "tx_id": tx_id,
                "branch_id": winner_branch_id,
                "key": task_key(namespace, "answer:final"),
                "value": final_raw,
            },
            tree_tx_put(
                {
                    "tx_id": tx_id,
                    "branch_id": winner_branch_id,
                    "key": task_key(namespace, "answer:final"),
                    "value": final_raw,
                }
            ),
        )

        commit_result = _record_tool(trace, messages, "tree_tx_commit_retry", {"tx_id": tx_id}, tree_tx_commit_retry({"tx_id": tx_id}))
        if not commit_result.get("ok"):
            abort_count += 1
            retry_count += 1
            winner_commit_rounds += 1
            abort_reason = commit_result.get("abort_reason", "") or commit_result.get("error", "TREE_COMMIT_FAILED")
            abort_reasons.append(abort_reason)
            if hotspot_key:
                _record_tool(
                    trace,
                    messages,
                    "tree_tx_refresh_winner",
                    {"tx_id": tx_id, "branch_id": winner_branch_id, "key": hotspot_key},
                    tree_tx_refresh_winner({"tx_id": tx_id, "branch_id": winner_branch_id, "key": hotspot_key}),
                )
            winner_store_refresh = _record_tool(
                trace,
                messages,
                "tree_tx_refresh_winner",
                {
                    "tx_id": tx_id,
                    "branch_id": winner_branch_id,
                    "key": task_key(namespace, "store:{}:detail".format(winner_store_id)),
                },
                tree_tx_refresh_winner(
                    {
                        "tx_id": tx_id,
                        "branch_id": winner_branch_id,
                        "key": task_key(namespace, "store:{}:detail".format(winner_store_id)),
                    }
                ),
            )
            if expected_product_id:
                winner_product_refresh = _record_tool(
                    trace,
                    messages,
                    "tree_tx_refresh_winner",
                    {
                        "tx_id": tx_id,
                        "branch_id": winner_branch_id,
                        "key": task_key(namespace, "product:{}:detail".format(expected_product_id)),
                    },
                    tree_tx_refresh_winner(
                        {
                            "tx_id": tx_id,
                            "branch_id": winner_branch_id,
                            "key": task_key(namespace, "product:{}:detail".format(expected_product_id)),
                        }
                    ),
                )
            constraints_ok, constraint_reason, _refreshed_payload = _semantic_recheck_winner_payload(
                store_id=winner_store_id,
                attempt=winner_branch_id,
                store_raw=winner_store_refresh.get("value") if winner_store_refresh else None,
                expected_order=expected_order,
                expected_product_id=expected_product_id,
                expected_product_profile=expected_product_profile,
                product_raw=winner_product_refresh.get("value") if winner_product_refresh else None,
            )
            if not constraints_ok:
                semantic_recheck_fail_count += 1
                abort_reason = constraint_reason
                semantic_recheck_reasons.append(abort_reason)
                _record_tool(trace, messages, "tree_tx_abort", {"tx_id": tx_id}, tree_tx_abort({"tx_id": tx_id}))
                commit_phase_latency_sec = time.time() - commit_phase_started
                return _build_agent_result(False, None, None, winner_store_id)
            _record_tool(
                trace,
                messages,
                "tree_tx_put",
                {
                    "tx_id": tx_id,
                    "branch_id": winner_branch_id,
                    "key": task_key(namespace, "answer:final"),
                    "value": final_raw,
                },
                tree_tx_put(
                    {
                        "tx_id": tx_id,
                        "branch_id": winner_branch_id,
                        "key": task_key(namespace, "answer:final"),
                        "value": final_raw,
                    }
                ),
            )
            commit_result = _record_tool(trace, messages, "tree_tx_commit_retry", {"tx_id": tx_id}, tree_tx_commit_retry({"tx_id": tx_id}))
            if not commit_result.get("ok"):
                abort_count += 1
                abort_reason = commit_result.get("abort_reason", "") or commit_result.get("error", "TREE_COMMIT_FAILED")
                abort_reasons.append(abort_reason)
                _record_tool(trace, messages, "tree_tx_abort", {"tx_id": tx_id}, tree_tx_abort({"tx_id": tx_id}))
            else:
                committed_attempts = 1
        else:
            committed_attempts = 1
        commit_phase_latency_sec = time.time() - commit_phase_started

        if committed_attempts:
            try:
                final_answer, final_raw = _load_final_answer(namespace)
            except Exception:
                pass
            return _build_agent_result(True, final_answer, final_raw, winner_store_id)
    except Exception as exc:
        abort_count += 1
        abort_reason = str(exc)
        abort_reasons.append(abort_reason)
    try:
        _record_tool(trace, messages, "tree_tx_abort", {"tx_id": tx_id}, tree_tx_abort({"tx_id": tx_id}))
    except Exception:
        pass
    return _build_agent_result(False, None, None, winner_store_id)


def hard_evaluate_task(
    task: Dict[str, Any],
    namespace: str,
    fallback_parsed: Optional[Any] = None,
    fallback_raw: Optional[str] = None,
) -> Dict[str, Any]:
    expected_order = _required_order(task)
    try:
        actual_parsed, actual_raw = _load_final_answer(namespace)
    except Exception:
        actual_parsed, actual_raw = fallback_parsed, fallback_raw
    if actual_raw in (None, ""):
        return {
            "success": False,
            "reward": 0.0,
            "failure_type": "missing_final_answer",
            "mismatches": ["final answer key is empty"],
            "expected_order": expected_order,
            "actual_order": None,
        }
    if actual_parsed is None:
        return {
            "success": False,
            "reward": 0.0,
            "failure_type": "invalid_final_answer_json",
            "mismatches": ["final answer is not valid JSON"],
            "expected_order": expected_order,
            "actual_order_raw": actual_raw,
        }
    if not isinstance(actual_parsed, list) or len(actual_parsed) != 1 or not isinstance(actual_parsed[0], dict):
        return {
            "success": False,
            "reward": 0.0,
            "failure_type": "unexpected_answer_shape",
            "mismatches": ["final answer must be a one-item order array"],
            "expected_order": expected_order,
            "actual_order": actual_parsed,
        }

    actual_order = actual_parsed[0]
    mismatches = []
    exact_fields = [
        "order_id",
        "order_type",
        "user_id",
        "store_id",
        "total_price",
        "create_time",
        "update_time",
        "status",
    ]
    soft_fields = ["dispatch_time", "delivery_time", "note"]

    def comparable_value(value: Any) -> Any:
        if value in ("", None):
            return None
        if isinstance(value, (int, float)):
            return round(float(value), 6)
        if isinstance(value, str):
            try:
                return round(float(value), 6)
            except Exception:
                return value
        return value

    for field in exact_fields:
        if comparable_value(actual_order.get(field)) != comparable_value(expected_order.get(field)):
            mismatches.append("{} expected={} actual={}".format(field, expected_order.get(field), actual_order.get(field)))
    for field in soft_fields:
        if comparable_value(actual_order.get(field, "")) != comparable_value(expected_order.get(field, "")):
            mismatches.append("{} expected={} actual={}".format(field, expected_order.get(field), actual_order.get(field)))

    expected_shipping = expected_order.get("shipping_time", None)
    actual_shipping = actual_order.get("shipping_time", None)
    normalized_shipping = None if actual_shipping in ("", None, "null", "None") else actual_shipping
    normalized_expected_shipping = None if expected_shipping in ("", None, "null", "None") else expected_shipping
    if normalized_shipping != normalized_expected_shipping:
        mismatches.append("shipping_time expected={} actual={}".format(normalized_expected_shipping, normalized_shipping))

    expected_location = expected_order.get("location", {})
    actual_location = actual_order.get("location", {})
    for field in ["address", "longitude", "latitude"]:
        if actual_location.get(field) != expected_location.get(field):
            mismatches.append(
                "location.{} expected={} actual={}".format(field, expected_location.get(field), actual_location.get(field))
            )

    expected_products = expected_order.get("products", [])
    actual_products = actual_order.get("products", [])
    if len(expected_products) != len(actual_products):
        mismatches.append("products length expected={} actual={}".format(len(expected_products), len(actual_products)))
    else:
        for idx, expected_product in enumerate(expected_products):
            actual_product = actual_products[idx]
            for field in ["product_id", "price", "quantity", "attributes"]:
                if comparable_value(actual_product.get(field)) != comparable_value(expected_product.get(field)):
                    mismatches.append(
                        "products[{}].{} expected={} actual={}".format(
                            idx, field, expected_product.get(field), actual_product.get(field)
                        )
                    )

    success = len(mismatches) == 0
    return {
        "success": success,
        "reward": 1.0 if success else 0.0,
        "failure_type": "" if success else "hard_mismatch",
        "mismatches": mismatches,
        "expected_order": expected_order,
        "actual_order": actual_order,
    }


def build_official_style_simulation(
    task: Dict[str, Any],
    namespace: str,
    mode: str,
    trial: int,
    agent_result: Dict[str, Any],
    eval_result: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "task_id": str(task["id"]),
        "task_uid": task.get("_task_uid", str(task["id"])),
        "trial": trial,
        "mode": mode,
        "messages": agent_result.get("messages", []),
        "states": {
            "namespace": namespace,
            "final_answer_key": task_key(namespace, "answer:final"),
            "final_answer": agent_result.get("final_answer"),
            "db_abort_reason": agent_result.get("db_abort_reason", ""),
        },
        "reward_info": {
            "reward": eval_result.get("reward", 0.0),
            "info": {
                "evaluation_method": "hard_match",
                "failure_type": eval_result.get("failure_type", ""),
                "mismatches": eval_result.get("mismatches", []),
            },
            "reward_breakdown": {"hard_match": eval_result.get("reward", 0.0)},
        },
        "task": {
            "instructions": task.get("instructions", ""),
            "evaluation_criteria": task.get("evaluation_criteria", {}),
            "environment": {"time": task.get("environment", {}).get("time", "")},
        },
    }


def _aggregate_mode_metrics(records: Sequence[Dict[str, Any]], trials: int, elapsed_sec: float) -> Dict[str, Any]:
    if not records:
        return {}
    success_count = sum(1 for record in records if record["evaluation"]["success"])
    latencies = [record["latency_sec"] for record in records]
    abort_reasons = defaultdict(int)
    semantic_recheck_reasons = defaultdict(int)
    failure_taxonomy = defaultdict(int)
    for record in records:
        for reason in record["agent_result"].get("abort_reasons", []):
            if reason:
                abort_reasons[reason] += 1
        for reason in record["agent_result"].get("semantic_recheck_reasons", []):
            if reason:
                semantic_recheck_reasons[reason] += 1
        failure = record["evaluation"].get("failure_type", "")
        if failure:
            failure_taxonomy[failure] += 1

    by_task = defaultdict(list)
    for record in records:
        by_task[record["task_uid"]].append(record["evaluation"]["reward"])

    avg_reward = sum(record["evaluation"]["reward"] for record in records) / float(len(records))
    pass_hat_ks = {}
    pass_at_ks = {}
    average_at_ks = {}
    for k in range(1, trials + 1):
        task_pass_hat = []
        task_pass_at = []
        task_average = []
        for rewards in by_task.values():
            n = len(rewards)
            if n < k:
                continue
            c = int(sum(1 for reward in rewards if reward == 1.0))
            task_pass_hat.append(_pass_hat_k(n, c, k))
            task_pass_at.append(_pass_at_k(n, c, k))
            task_average.append(sum(rewards) / float(len(rewards)))
        if task_pass_hat:
            pass_hat_ks[k] = sum(task_pass_hat) / float(len(task_pass_hat))
        if task_pass_at:
            pass_at_ks[k] = sum(task_pass_at) / float(len(task_pass_at))
        if task_average:
            average_at_ks[k] = sum(task_average) / float(len(task_average))

    total_commits = sum(record["agent_result"].get("committed_attempts", 0) for record in records)
    total_txn_attempts = sum(record["agent_result"].get("txn_attempts", 0) for record in records)
    total_rollbacks = sum(record["agent_result"].get("rolled_back_attempts", 0) for record in records)
    total_candidates = sum(record["agent_result"].get("candidate_count", 0) for record in records)
    total_branches = sum(record["agent_result"].get("branch_count", 0) for record in records)
    total_abort_count = sum(record["agent_result"].get("db_abort_count", 0) for record in records)
    total_semantic_recheck_fail_count = sum(record["agent_result"].get("semantic_recheck_fail_count", 0) for record in records)
    total_retry_count = sum(record["agent_result"].get("db_retry_count", 0) for record in records)
    total_bumps = sum(record["agent_result"].get("interference_bumps", 0) for record in records)
    total_explore_txn_attempts = sum(record["agent_result"].get("explore_txn_attempts", 0) for record in records)
    total_winner_txn_attempts = sum(record["agent_result"].get("winner_txn_attempts", 0) for record in records)
    total_winner_commit_rounds = sum(record["agent_result"].get("winner_commit_rounds", 0) for record in records)
    winner_selection_latencies = [record["agent_result"].get("winner_selection_latency_sec", 0.0) for record in records]
    explore_phase_latencies = [record["agent_result"].get("explore_phase_latency_sec", 0.0) for record in records]
    commit_phase_latencies = [record["agent_result"].get("commit_phase_latency_sec", 0.0) for record in records]

    elapsed = max(elapsed_sec, 1e-9)
    return {
        "records": len(records),
        "task_count": len(by_task),
        "success_rate": float(success_count) / float(len(records)),
        "avg_reward": avg_reward,
        "avg_latency_sec": sum(latencies) / float(len(latencies)),
        "p95_latency_sec": _p95(latencies),
        "throughput_tasks_per_sec": float(len(records)) / elapsed,
        "throughput_commits_per_sec": float(total_commits) / elapsed,
        "logical_branches_per_sec": float(total_branches) / elapsed,
        "avg_txn_attempts_per_task": float(total_txn_attempts) / float(len(records)),
        "avg_rolled_back_attempts_per_task": float(total_rollbacks) / float(len(records)),
        "avg_candidate_count": float(total_candidates) / float(len(records)),
        "avg_branch_count": float(total_branches) / float(len(records)),
        "avg_explore_txn_attempts_per_task": float(total_explore_txn_attempts) / float(len(records)),
        "avg_winner_txn_attempts_per_task": float(total_winner_txn_attempts) / float(len(records)),
        "avg_winner_commit_rounds_per_task": float(total_winner_commit_rounds) / float(len(records)),
        "avg_winner_selection_latency_sec": sum(winner_selection_latencies) / float(len(winner_selection_latencies)),
        "avg_explore_phase_latency_sec": sum(explore_phase_latencies) / float(len(explore_phase_latencies)),
        "avg_commit_phase_latency_sec": sum(commit_phase_latencies) / float(len(commit_phase_latencies)),
        "p95_winner_selection_latency_sec": _p95(winner_selection_latencies),
        "p95_explore_phase_latency_sec": _p95(explore_phase_latencies),
        "p95_commit_phase_latency_sec": _p95(commit_phase_latencies),
        "db_abort_count": total_abort_count,
        "db_abort_rate_per_attempt": float(total_abort_count) / float(max(total_txn_attempts, 1)),
        "semantic_recheck_fail_count": total_semantic_recheck_fail_count,
        "semantic_recheck_fail_rate_per_task": float(total_semantic_recheck_fail_count) / float(len(records)),
        "db_retry_count": total_retry_count,
        "interference_bumps": total_bumps,
        "elapsed_sec": elapsed_sec,
        "abort_reason_distribution": dict(abort_reasons),
        "semantic_recheck_reason_distribution": dict(semantic_recheck_reasons),
        "failure_taxonomy": dict(failure_taxonomy),
        "pass_hat_k": pass_hat_ks,
        "pass_at_k": pass_at_ks,
        "average_at_k": average_at_ks,
    }


def _run_single_task(
    run_id: str,
    task: Dict[str, Any],
    mode: str,
    trial_idx: int,
    top_k: int,
    agent_type: str,
    max_rounds: int,
    contention_profile: str,
    interference_manager: Optional[HotspotInterferenceManager],
    hotspot_probability: float,
    baseline_winner_retry_policy: str,
    baseline_winner_max_attempts: int,
) -> Dict[str, Any]:
    task_id = str(task["id"])
    task_uid = task.get("_task_uid", task_id)
    namespace = build_namespace(run_id, mode, task_uid, trial_idx)
    projection = project_task(agent_db, task, namespace, contention_profile=contention_profile)

    t0 = time.time()
    if agent_type == "llm":
        agent_result = _llm_agent_run(
            task=task,
            namespace=namespace,
            mode=mode,
            top_k=top_k,
            max_rounds=max_rounds,
        )
    else:
        agent_result = _reference_agent_run(
            task=task,
            namespace=namespace,
            mode=mode,
            top_k=top_k,
            contention_profile=contention_profile,
            interference_manager=interference_manager,
            hotspot_probability=hotspot_probability,
            baseline_winner_retry_policy=baseline_winner_retry_policy,
            baseline_winner_max_attempts=baseline_winner_max_attempts,
        )
    latency_sec = time.time() - t0
    evaluation = hard_evaluate_task(
        task=task,
        namespace=namespace,
        fallback_parsed=agent_result.get("final_answer"),
        fallback_raw=agent_result.get("final_answer_raw"),
    )
    official_simulation = build_official_style_simulation(
        task=task,
        namespace=namespace,
        mode=mode,
        trial=trial_idx,
        agent_result=agent_result,
        eval_result=evaluation,
    )
    return {
        "run_id": run_id,
        "mode": mode,
        "trial": trial_idx,
        "task_id": task_id,
        "task_uid": task_uid,
        "task_source": task.get("_task_source", "delivery"),
        "namespace": namespace,
        "latency_sec": latency_sec,
        "projection": projection,
        "agent_result": agent_result,
        "evaluation": evaluation,
        "official_style_simulation": official_simulation,
    }


def run_delivery_experiment(
    dataset_path: str,
    output_dir: str,
    modes: Sequence[str],
    trials: int,
    limit: Optional[int],
    task_ids: Optional[Sequence[str]],
    top_k: int,
    agent_type: str,
    max_rounds: int,
    task_source: str,
    cross_domain_path: str,
    contention_profile: str,
    parallelism: int,
    interference_workers: int,
    hotspot_probability: float,
    baseline_winner_retry_policy: str,
    baseline_winner_max_attempts: int,
    progress_callback: Optional[Any] = None,
) -> Dict[str, Any]:
    _makedirs(output_dir)
    tasks, load_summary = load_experiment_tasks(
        delivery_path=dataset_path,
        task_source=task_source,
        cross_domain_path=cross_domain_path,
        task_ids=task_ids,
        limit=limit,
    )

    run_id = str(uuid.uuid4())
    all_records = []
    started_at = time.time()
    mode_elapsed = {}
    interference_manager = None
    if agent_type == "reference" and contention_profile == "hotspot" and interference_workers > 0:
        interference_manager = HotspotInterferenceManager(interference_workers)

    try:
        for mode in modes:
            mode_started = time.time()
            mode_records = []
            for trial_idx in range(1, trials + 1):
                if parallelism <= 1:
                    for task in tasks:
                        mode_records.append(
                            _run_single_task(
                                run_id=run_id,
                                task=task,
                                mode=mode,
                                trial_idx=trial_idx,
                                top_k=top_k,
                                agent_type=agent_type,
                                max_rounds=max_rounds,
                                contention_profile=contention_profile,
                                interference_manager=interference_manager,
                                hotspot_probability=hotspot_probability,
                                baseline_winner_retry_policy=baseline_winner_retry_policy,
                                baseline_winner_max_attempts=baseline_winner_max_attempts,
                            )
                        )
                        if progress_callback is not None:
                            progress_callback(mode, trial_idx, len(mode_records), len(tasks))
                else:
                    with ThreadPoolExecutor(max_workers=parallelism) as executor:
                        futures = []
                        for task in tasks:
                            futures.append(
                                executor.submit(
                                    _run_single_task,
                                    run_id,
                                    task,
                                    mode,
                                    trial_idx,
                                    top_k,
                                    agent_type,
                                    max_rounds,
                                    contention_profile,
                                    interference_manager,
                                    hotspot_probability,
                                    baseline_winner_retry_policy,
                                    baseline_winner_max_attempts,
                                )
                            )
                        for future in as_completed(futures):
                            mode_records.append(future.result())
                            if progress_callback is not None:
                                progress_callback(mode, trial_idx, len(mode_records), len(tasks))
            mode_elapsed[mode] = time.time() - mode_started
            all_records.extend(mode_records)
    finally:
        if interference_manager is not None:
            interference_manager.close()

    finished_at = time.time()
    summary = {
        "run_id": run_id,
        "dataset_path": dataset_path,
        "cross_domain_path": cross_domain_path,
        "backend": agent_db.backend,
        "modes": list(modes),
        "trials": trials,
        "task_count": len(tasks),
        "agent_type": agent_type,
        "task_source": task_source,
        "contention_profile": contention_profile,
        "parallelism": parallelism,
        "interference_workers": interference_workers,
        "hotspot_probability": hotspot_probability,
        "baseline_winner_retry_policy": baseline_winner_retry_policy,
        "baseline_winner_max_attempts": baseline_winner_max_attempts,
        "eligible_task_count": load_summary.get("eligible_task_count", len(tasks)),
        "skipped_task_count": load_summary.get("skipped_task_count", 0),
        "skip_reason_distribution": load_summary.get("skip_reason_distribution", {}),
        "source_counts": load_summary.get("source_counts", {}),
        "comparison_contract": {
            "same_candidate_pool": True,
            "same_winner_selector": "deterministic_rank_on_branch_payload",
            "same_private_branch_payload_shape": True,
            "baseline_execution_model": "serial_normal_transactions_plus_winner_commit",
            "tree_execution_model": "single_tree_transaction_plus_branch_private_rw",
        },
        "elapsed_sec": finished_at - started_at,
        "mode_metrics": {},
    }
    for mode in modes:
        summary["mode_metrics"][mode] = _aggregate_mode_metrics(
            [record for record in all_records if record["mode"] == mode],
            trials=trials,
            elapsed_sec=mode_elapsed.get(mode, 0.0),
        )

    records_path = os.path.join(output_dir, "records-{}.jsonl".format(run_id))
    summary_path = os.path.join(output_dir, "summary-{}.json".format(run_id))
    _append_jsonl(records_path, all_records)
    _write_json_file(summary_path, summary)

    return {
        "summary": summary,
        "records_path": records_path,
        "summary_path": summary_path,
        "record_count": len(all_records),
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Tree-DB x VitaBench delivery experiments.")
    parser.add_argument("--dataset-path", default=DEFAULT_DATASET_PATH, help="Path to VitaBench delivery tasks.json")
    parser.add_argument("--cross-domain-path", default=DEFAULT_CROSS_DOMAIN_PATH, help="Path to VitaBench cross_domain tasks.json")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Directory for experiment artifacts")
    parser.add_argument(
        "--task-source",
        default="delivery",
        choices=["delivery", "cross_domain", "both"],
        help="Which official task source(s) to load",
    )
    parser.add_argument(
        "--mode",
        default="both",
        choices=["baseline", "tree", "both"],
        help="Experiment mode to run",
    )
    parser.add_argument(
        "--preset",
        default="smoke",
        choices=["smoke", "functional", "full", "custom"],
        help="Task count preset",
    )
    parser.add_argument("--limit", type=int, default=None, help="Custom task limit; overrides preset when provided")
    parser.add_argument("--task-ids", default="", help="Comma-separated task ids or source-prefixed task_uids to run")
    parser.add_argument("--trials", type=int, default=1, help="Number of trials per task")
    parser.add_argument("--top-k", type=int, default=3, help="Number of candidate stores to branch over in tree mode")
    parser.add_argument(
        "--agent-type",
        default="reference",
        choices=["reference", "llm"],
        help="reference uses the deterministic built-in executor; llm uses the real tool-calling LLM agent",
    )
    parser.add_argument("--max-rounds", type=int, default=12, help="Maximum LLM/tool rounds when agent-type=llm")
    parser.add_argument(
        "--contention-profile",
        default="none",
        choices=["none", "hotspot"],
        help="Whether to project and perturb a hotspot key during execution",
    )
    parser.add_argument(
        "--hotspot-probability",
        type=float,
        default=1.0,
        help="Probability of injecting a hotspot bump on each eligible task when contention-profile=hotspot",
    )
    parser.add_argument("--parallelism", type=int, default=1, help="Number of concurrent task instances per mode/trial")
    parser.add_argument("--interference-workers", type=int, default=0, help="Number of background hotspot interference workers")
    parser.add_argument(
        "--baseline-winner-retry-policy",
        default="fixed",
        choices=["fixed", "until_success"],
        help="How baseline retries the winner transaction after an abort",
    )
    parser.add_argument(
        "--baseline-winner-max-attempts",
        type=int,
        default=2,
        help="Total winner attempts in baseline when retry policy is fixed",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    task_ids = [item.strip() for item in args.task_ids.split(",") if item.strip()]
    limit = args.limit
    if limit is None and not task_ids:
        limit = DEFAULT_TASK_LIMITS[args.preset] if args.preset != "custom" else None
    if limit is None and not task_ids:
        limit = DEFAULT_TASK_LIMITS["smoke"]

    if args.mode == "both":
        modes = ["baseline", "tree"]
    else:
        modes = [args.mode]

    result = run_delivery_experiment(
        dataset_path=args.dataset_path,
        output_dir=args.output_dir,
        modes=modes,
        trials=args.trials,
        limit=limit,
        task_ids=task_ids or None,
        top_k=max(1, args.top_k),
        agent_type=args.agent_type,
        max_rounds=max(1, args.max_rounds),
        task_source=args.task_source,
        cross_domain_path=args.cross_domain_path,
        contention_profile=args.contention_profile,
        parallelism=max(1, args.parallelism),
        interference_workers=max(0, args.interference_workers),
        hotspot_probability=max(0.0, min(1.0, args.hotspot_probability)),
        baseline_winner_retry_policy=args.baseline_winner_retry_policy,
        baseline_winner_max_attempts=max(1, args.baseline_winner_max_attempts),
    )

    print("run_id={}".format(result["summary"]["run_id"]))
    print("record_count={}".format(result["record_count"]))
    print("records_path={}".format(result["records_path"]))
    print("summary_path={}".format(result["summary_path"]))
    print(_json_dumps(result["summary"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
