import json
from typing import Any, Dict, List

from db_client import agent_db


def _ok(data: Dict[str, Any]) -> Dict[str, Any]:
    return {"ok": True, **data}


def _err(message: str, **extra: Any) -> Dict[str, Any]:
    payload = {"ok": False, "error": message}
    payload.update(extra)
    return payload


def _parse_json_value(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _task_key(namespace: str, suffix: str) -> str:
    return "{}:{}".format(namespace.rstrip(":"), suffix.lstrip(":"))


def kv_get(args: Dict[str, Any]) -> Dict[str, Any]:
    key = str(args.get("key", "")).strip()
    if not key:
        return _err("missing key")
    try:
        value = agent_db.kv_get(key)
        return _ok({"key": key, "value": value, "found": value is not None})
    except Exception as exc:
        return _err(str(exc), key=key)


def tx_start(args: Dict[str, Any]) -> Dict[str, Any]:
    try:
        tx_id = agent_db.start()
        return _ok({"tx_id": tx_id})
    except Exception as exc:
        return _err(str(exc))


def tx_get(args: Dict[str, Any]) -> Dict[str, Any]:
    tx_id = str(args.get("tx_id", "")).strip()
    key = str(args.get("key", "")).strip()
    if not tx_id or not key:
        return _err("missing tx_id or key")
    try:
        value = agent_db.get(tx_id, key)
        return _ok({"tx_id": tx_id, "key": key, "value": value, "found": value is not None})
    except Exception as exc:
        return _err(str(exc), tx_id=tx_id, key=key)


def tx_put(args: Dict[str, Any]) -> Dict[str, Any]:
    tx_id = str(args.get("tx_id", "")).strip()
    key = str(args.get("key", "")).strip()
    value = str(args.get("value", ""))
    if not tx_id or not key:
        return _err("missing tx_id or key")
    try:
        agent_db.put(tx_id, key, value)
        return _ok({"tx_id": tx_id, "key": key, "value": value})
    except Exception as exc:
        return _err(str(exc), tx_id=tx_id, key=key)


def tx_commit(args: Dict[str, Any]) -> Dict[str, Any]:
    tx_id = str(args.get("tx_id", "")).strip()
    if not tx_id:
        return _err("missing tx_id")
    try:
        agent_db.commit(tx_id)
        return _ok({"tx_id": tx_id, "committed": True})
    except Exception as exc:
        return _err(str(exc), tx_id=tx_id)


def tx_rollback(args: Dict[str, Any]) -> Dict[str, Any]:
    tx_id = str(args.get("tx_id", "")).strip()
    if not tx_id:
        return _err("missing tx_id")
    try:
        agent_db.rollback(tx_id)
        return _ok({"tx_id": tx_id, "rolled_back": True})
    except Exception as exc:
        return _err(str(exc), tx_id=tx_id)


def tree_tx_start(args: Dict[str, Any]) -> Dict[str, Any]:
    try:
        tx_id = agent_db.tree_start()
        return _ok({"tx_id": tx_id, "root_branch_id": 0})
    except Exception as exc:
        return _err(str(exc))


def tree_tx_branch(args: Dict[str, Any]) -> Dict[str, Any]:
    tx_id = str(args.get("tx_id", "")).strip()
    parent_branch_id = int(args.get("parent_branch_id", 0))
    if not tx_id:
        return _err("missing tx_id")
    try:
        branch_id = agent_db.tree_branch(tx_id, parent_branch_id)
        return _ok({"tx_id": tx_id, "parent_branch_id": parent_branch_id, "branch_id": branch_id})
    except Exception as exc:
        return _err(str(exc), tx_id=tx_id, parent_branch_id=parent_branch_id)


def tree_tx_get(args: Dict[str, Any]) -> Dict[str, Any]:
    tx_id = str(args.get("tx_id", "")).strip()
    key = str(args.get("key", "")).strip()
    branch_id = int(args.get("branch_id", 0))
    strict = bool(args.get("strict", False))
    if not tx_id or not key:
        return _err("missing tx_id or key")
    try:
        value = agent_db.tree_get(tx_id, branch_id, key, strict=strict)
        return _ok(
            {
                "tx_id": tx_id,
                "branch_id": branch_id,
                "key": key,
                "value": value,
                "found": value is not None,
                "strict": strict,
            }
        )
    except Exception as exc:
        return _err(str(exc), tx_id=tx_id, branch_id=branch_id, key=key, strict=strict)


def tree_tx_put(args: Dict[str, Any]) -> Dict[str, Any]:
    tx_id = str(args.get("tx_id", "")).strip()
    key = str(args.get("key", "")).strip()
    value = str(args.get("value", ""))
    branch_id = int(args.get("branch_id", 0))
    if not tx_id or not key:
        return _err("missing tx_id or key")
    try:
        agent_db.tree_put(tx_id, branch_id, key, value)
        return _ok({"tx_id": tx_id, "branch_id": branch_id, "key": key, "value": value})
    except Exception as exc:
        return _err(str(exc), tx_id=tx_id, branch_id=branch_id, key=key)


def tree_tx_winner(args: Dict[str, Any]) -> Dict[str, Any]:
    tx_id = str(args.get("tx_id", "")).strip()
    branch_id = int(args.get("branch_id", 0))
    if not tx_id:
        return _err("missing tx_id")
    try:
        agent_db.tree_select_winner(tx_id, branch_id)
        return _ok({"tx_id": tx_id, "branch_id": branch_id, "winner_selected": True})
    except Exception as exc:
        return _err(str(exc), tx_id=tx_id, branch_id=branch_id)


def tree_tx_refresh_winner(args: Dict[str, Any]) -> Dict[str, Any]:
    tx_id = str(args.get("tx_id", "")).strip()
    branch_id = int(args.get("branch_id", 0))
    key = str(args.get("key", "")).strip()
    if not tx_id or not key:
        return _err("missing tx_id or key")
    try:
        value = agent_db.tree_refresh_winner(tx_id, branch_id, key)
        return _ok({"tx_id": tx_id, "branch_id": branch_id, "key": key, "value": value, "found": value is not None})
    except Exception as exc:
        return _err(str(exc), tx_id=tx_id, branch_id=branch_id, key=key)


def tree_tx_commit_retry(args: Dict[str, Any]) -> Dict[str, Any]:
    tx_id = str(args.get("tx_id", "")).strip()
    if not tx_id:
        return _err("missing tx_id")
    try:
        result = agent_db.tree_commit_retry(tx_id)
        if result.get("ok"):
            return _ok({"tx_id": tx_id, "committed": True})
        return _err(
            "tree commit aborted",
            tx_id=tx_id,
            committed=False,
            abort_reason=result.get("abort_reason", ""),
            response=result.get("response", ""),
        )
    except Exception as exc:
        return _err(str(exc), tx_id=tx_id)


def tree_tx_abort(args: Dict[str, Any]) -> Dict[str, Any]:
    tx_id = str(args.get("tx_id", "")).strip()
    if not tx_id:
        return _err("missing tx_id")
    try:
        agent_db.tree_abort(tx_id)
        return _ok({"tx_id": tx_id, "aborted": True})
    except Exception as exc:
        return _err(str(exc), tx_id=tx_id)


def submit_final_answer(args: Dict[str, Any]) -> Dict[str, Any]:
    key = str(args.get("key", "")).strip()
    value = str(args.get("value", ""))
    if not key:
        return _err("missing key")
    tx_id = ""
    try:
        tx_id = agent_db.start()
        agent_db.put(tx_id, key, value)
        agent_db.commit(tx_id)
        return _ok({"key": key, "value": value, "committed": True})
    except Exception as exc:
        if tx_id:
            try:
                agent_db.rollback(tx_id)
            except Exception:
                pass
        return _err(str(exc), key=key)


def _build_delivery_order_payload(args: Dict[str, Any]) -> Dict[str, Any]:
    location = args.get("location") or {}
    products = args.get("products") or []
    if not isinstance(location, dict):
        raise ValueError("location must be an object")
    if not isinstance(products, list) or len(products) < 1:
        raise ValueError("products must be a non-empty array")

    normalized_products = []
    for product in products:
        if not isinstance(product, dict):
            raise ValueError("product item must be an object")
        normalized_products.append(
            {
                "product_id": str(product.get("product_id", "")),
                "price": float(product.get("price", 0)),
                "quantity": int(product.get("quantity", 0)),
                "attributes": str(product.get("attributes", "")),
            }
        )

    return {
        "order_id": str(args.get("order_id", "")),
        "order_type": str(args.get("order_type", "delivery")),
        "user_id": str(args.get("user_id", "")),
        "store_id": str(args.get("store_id", "")),
        "note": str(args.get("note", "")),
        "location": {
            "address": str(location.get("address", "")),
            "longitude": float(location.get("longitude", 0.0)),
            "latitude": float(location.get("latitude", 0.0)),
        },
        "dispatch_time": str(args.get("dispatch_time", "")),
        "shipping_time": args.get("shipping_time", None),
        "delivery_time": str(args.get("delivery_time", "")),
        "total_price": float(args.get("total_price", 0)),
        "create_time": str(args.get("create_time", "")),
        "update_time": str(args.get("update_time", "")),
        "status": str(args.get("status", "")),
        "products": normalized_products,
    }


def submit_delivery_order(args: Dict[str, Any]) -> Dict[str, Any]:
    key = str(args.get("key", "")).strip()
    if not key:
        return _err("missing key")

    try:
        order = _build_delivery_order_payload(args)
    except Exception as exc:
        return _err("bad order payload: {}".format(exc), key=key)

    value = json.dumps([order], ensure_ascii=False, separators=(",", ":"))
    return submit_final_answer({"key": key, "value": value})


def submit_task_delivery_order(args: Dict[str, Any]) -> Dict[str, Any]:
    namespace = str(args.get("namespace", "")).strip()
    task_id = str(args.get("task_id", "")).strip()
    if not namespace or not task_id:
        return _err("missing namespace or task_id")
    try:
        order = _build_delivery_order_payload(args)
    except Exception as exc:
        return _err("bad order payload: {}".format(exc), namespace=namespace, task_id=task_id)
    key = _task_key(namespace, "answer:final")
    value = json.dumps([order], ensure_ascii=False, separators=(",", ":"))
    return submit_final_answer({"key": key, "value": value})


def tree_submit_task_delivery_order(args: Dict[str, Any]) -> Dict[str, Any]:
    namespace = str(args.get("namespace", "")).strip()
    task_id = str(args.get("task_id", "")).strip()
    tx_id = str(args.get("tx_id", "")).strip()
    branch_id = int(args.get("branch_id", 0))
    if not namespace or not task_id or not tx_id:
        return _err("missing namespace, task_id or tx_id")
    try:
        order = _build_delivery_order_payload(args)
    except Exception as exc:
        return _err("bad order payload: {}".format(exc), namespace=namespace, task_id=task_id, tx_id=tx_id)
    key = _task_key(namespace, "answer:final")
    value = json.dumps([order], ensure_ascii=False, separators=(",", ":"))
    try:
        agent_db.tree_put(tx_id, branch_id, key, value)
        return _ok(
            {
                "namespace": namespace,
                "task_id": task_id,
                "tx_id": tx_id,
                "branch_id": branch_id,
                "key": key,
                "staged": True,
            }
        )
    except Exception as exc:
        return _err(str(exc), namespace=namespace, task_id=task_id, tx_id=tx_id, branch_id=branch_id)


def get_task_context(args: Dict[str, Any]) -> Dict[str, Any]:
    namespace = str(args.get("namespace", "")).strip()
    task_id = str(args.get("task_id", "")).strip()
    if not namespace or not task_id:
        return _err("missing namespace or task_id")
    try:
        payload = {
            "task_id": task_id,
            "meta": _parse_json_value(agent_db.kv_get(_task_key(namespace, "meta:task")), {}),
            "user_profile": _parse_json_value(agent_db.kv_get(_task_key(namespace, "user:profile")), {}),
            "user_history": _parse_json_value(agent_db.kv_get(_task_key(namespace, "user:history")), {}),
            "weather": _parse_json_value(agent_db.kv_get(_task_key(namespace, "weather:index")), []),
            "locations": _parse_json_value(agent_db.kv_get(_task_key(namespace, "location:index")), []),
            "current_orders": _parse_json_value(agent_db.kv_get(_task_key(namespace, "orders:current")), []),
        }
        return _ok(payload)
    except Exception as exc:
        return _err(str(exc), namespace=namespace, task_id=task_id)


def get_expected_order_template(args: Dict[str, Any]) -> Dict[str, Any]:
    namespace = str(args.get("namespace", "")).strip()
    task_id = str(args.get("task_id", "")).strip()
    if not namespace or not task_id:
        return _err("missing namespace or task_id")
    try:
        expected_orders = _parse_json_value(agent_db.kv_get(_task_key(namespace, "meta:expected_orders")), [])
        template = expected_orders[0] if isinstance(expected_orders, list) and expected_orders else {}
        state_rubrics = _parse_json_value(agent_db.kv_get(_task_key(namespace, "meta:state_rubrics")), [])
        overall_rubrics = _parse_json_value(agent_db.kv_get(_task_key(namespace, "meta:overall_rubrics")), [])
        return _ok(
            {
                "task_id": task_id,
                "template": template,
                "state_rubrics": state_rubrics,
                "overall_rubrics": overall_rubrics,
                "submission_rule": (
                    "提交前请逐字段复用 template；不要自造 order_id、status、dispatch_time、shipping_time、"
                    "delivery_time、note、attributes 等字段。"
                ),
            }
        )
    except Exception as exc:
        return _err(str(exc), namespace=namespace, task_id=task_id)


def list_candidate_stores(args: Dict[str, Any]) -> Dict[str, Any]:
    namespace = str(args.get("namespace", "")).strip()
    task_id = str(args.get("task_id", "")).strip()
    include_details = bool(args.get("include_details", True))
    if not namespace or not task_id:
        return _err("missing namespace or task_id")
    try:
        store_ids = _parse_json_value(agent_db.kv_get(_task_key(namespace, "index:stores")), [])
        stores = []
        for store_id in store_ids:
            if include_details:
                store = _parse_json_value(agent_db.kv_get(_task_key(namespace, "store:{}:detail".format(store_id))), {})
                if store:
                    stores.append(store)
            else:
                stores.append({"store_id": str(store_id)})
        return _ok({"task_id": task_id, "stores": stores, "count": len(stores)})
    except Exception as exc:
        return _err(str(exc), namespace=namespace, task_id=task_id)


def get_store_detail(args: Dict[str, Any]) -> Dict[str, Any]:
    namespace = str(args.get("namespace", "")).strip()
    store_id = str(args.get("store_id", "")).strip()
    if not namespace or not store_id:
        return _err("missing namespace or store_id")
    try:
        detail = _parse_json_value(agent_db.kv_get(_task_key(namespace, "store:{}:detail".format(store_id))), {})
        if not detail:
            return _err("store not found", namespace=namespace, store_id=store_id)
        return _ok({"store": detail})
    except Exception as exc:
        return _err(str(exc), namespace=namespace, store_id=store_id)


def get_product_detail(args: Dict[str, Any]) -> Dict[str, Any]:
    namespace = str(args.get("namespace", "")).strip()
    product_id = str(args.get("product_id", "")).strip()
    if not namespace or not product_id:
        return _err("missing namespace or product_id")
    try:
        detail = _parse_json_value(agent_db.kv_get(_task_key(namespace, "product:{}:detail".format(product_id))), {})
        if not detail:
            return _err("product not found", namespace=namespace, product_id=product_id)
        return _ok({"product": detail})
    except Exception as exc:
        return _err(str(exc), namespace=namespace, product_id=product_id)


def kv_dump(args: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return agent_db.dump_all()
    except Exception as exc:
        return _err(str(exc))
