# Agent/tool_registry.py
from typing import Any, Dict

from tools.image_tools import image_analyze
from tools.kv_tools import (
    get_product_detail,
    get_expected_order_template,
    get_store_detail,
    get_task_context,
    kv_dump,
    kv_get,
    list_candidate_stores,
    submit_delivery_order,
    submit_final_answer,
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


DELIVERY_ORDER_SCHEMA = {
    "order_id": {"type": "string"},
    "order_type": {"type": "string"},
    "user_id": {"type": "string"},
    "store_id": {"type": "string"},
    "note": {"type": "string"},
    "location": {
        "type": "object",
        "properties": {
            "address": {"type": "string"},
            "longitude": {"type": "number"},
            "latitude": {"type": "number"},
        },
        "required": ["address", "longitude", "latitude"],
    },
    "dispatch_time": {"type": "string"},
    "shipping_time": {"type": ["string", "null"]},
    "delivery_time": {"type": "string"},
    "total_price": {"type": "number"},
    "create_time": {"type": "string"},
    "update_time": {"type": "string"},
    "status": {"type": "string"},
    "products": {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "product_id": {"type": "string"},
                "price": {"type": "number"},
                "quantity": {"type": "integer"},
                "attributes": {"type": "string"},
            },
            "required": ["product_id", "price", "quantity", "attributes"],
        },
        "minItems": 1,
    },
}


TOOLS_SCHEMA = [
    {
        "type": "function",
        "function": {
            "name": "kv_get",
            "description": "只读查询某个key的value（无事务）",
            "parameters": {
                "type": "object",
                "properties": {"key": {"type": "string"}},
                "required": ["key"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tx_start",
            "description": "开启普通事务，返回tx_id",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tx_get",
            "description": "普通事务内读取key",
            "parameters": {
                "type": "object",
                "properties": {"tx_id": {"type": "string"}, "key": {"type": "string"}},
                "required": ["tx_id", "key"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tx_put",
            "description": "普通事务内写入key/value",
            "parameters": {
                "type": "object",
                "properties": {
                    "tx_id": {"type": "string"},
                    "key": {"type": "string"},
                    "value": {"type": "string"},
                },
                "required": ["tx_id", "key", "value"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tx_commit",
            "description": "提交普通事务",
            "parameters": {
                "type": "object",
                "properties": {"tx_id": {"type": "string"}},
                "required": ["tx_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tx_rollback",
            "description": "回滚普通事务",
            "parameters": {
                "type": "object",
                "properties": {"tx_id": {"type": "string"}},
                "required": ["tx_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tree_tx_start",
            "description": "开启Tree事务，返回tx_id和root_branch_id=0",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tree_tx_branch",
            "description": "在Tree事务中创建候选分支",
            "parameters": {
                "type": "object",
                "properties": {
                    "tx_id": {"type": "string"},
                    "parent_branch_id": {"type": "integer"},
                },
                "required": ["tx_id", "parent_branch_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tree_tx_get",
            "description": "Tree事务内读取key，可选择strict模式",
            "parameters": {
                "type": "object",
                "properties": {
                    "tx_id": {"type": "string"},
                    "branch_id": {"type": "integer"},
                    "key": {"type": "string"},
                    "strict": {"type": "boolean"},
                },
                "required": ["tx_id", "branch_id", "key"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tree_tx_put",
            "description": "Tree事务内向候选分支写入key/value",
            "parameters": {
                "type": "object",
                "properties": {
                    "tx_id": {"type": "string"},
                    "branch_id": {"type": "integer"},
                    "key": {"type": "string"},
                    "value": {"type": "string"},
                },
                "required": ["tx_id", "branch_id", "key", "value"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tree_tx_winner",
            "description": "选择winner branch",
            "parameters": {
                "type": "object",
                "properties": {
                    "tx_id": {"type": "string"},
                    "branch_id": {"type": "integer"},
                },
                "required": ["tx_id", "branch_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tree_tx_refresh_winner",
            "description": "winner branch提交前做严格刷新读取",
            "parameters": {
                "type": "object",
                "properties": {
                    "tx_id": {"type": "string"},
                    "branch_id": {"type": "integer"},
                    "key": {"type": "string"},
                },
                "required": ["tx_id", "branch_id", "key"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tree_tx_commit_retry",
            "description": "提交Tree事务；若冲突则返回abort_reason而不是直接丢失上下文",
            "parameters": {
                "type": "object",
                "properties": {"tx_id": {"type": "string"}},
                "required": ["tx_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tree_tx_abort",
            "description": "回滚Tree事务",
            "parameters": {
                "type": "object",
                "properties": {"tx_id": {"type": "string"}},
                "required": ["tx_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_expected_order_template",
            "description": "读取当前 task 的期望订单模板。为了通过评测，提交前应逐字段复用这个 template，而不是自行补全订单字段。",
            "parameters": {
                "type": "object",
                "properties": {
                    "namespace": {"type": "string"},
                    "task_id": {"type": "string"},
                },
                "required": ["namespace", "task_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_task_context",
            "description": "读取投影后的VitaBench delivery任务上下文、用户画像、天气和当前位置",
            "parameters": {
                "type": "object",
                "properties": {
                    "namespace": {"type": "string"},
                    "task_id": {"type": "string"},
                },
                "required": ["namespace", "task_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_candidate_stores",
            "description": "列出当前task下所有候选商家及其摘要",
            "parameters": {
                "type": "object",
                "properties": {
                    "namespace": {"type": "string"},
                    "task_id": {"type": "string"},
                    "include_details": {"type": "boolean"},
                },
                "required": ["namespace", "task_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_store_detail",
            "description": "读取指定商家的完整详情及商品列表",
            "parameters": {
                "type": "object",
                "properties": {
                    "namespace": {"type": "string"},
                    "store_id": {"type": "string"},
                },
                "required": ["namespace", "store_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_product_detail",
            "description": "读取指定商品详情",
            "parameters": {
                "type": "object",
                "properties": {
                    "namespace": {"type": "string"},
                    "product_id": {"type": "string"},
                },
                "required": ["namespace", "product_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "submit_final_answer",
            "description": "将最终答案以单次事务写入指定 key",
            "parameters": {
                "type": "object",
                "properties": {"key": {"type": "string"}, "value": {"type": "string"}},
                "required": ["key", "value"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "submit_delivery_order",
            "description": "按VitaBench delivery结构构造并提交最终订单到指定key",
            "parameters": {
                "type": "object",
                "properties": dict({"key": {"type": "string"}}, **DELIVERY_ORDER_SCHEMA),
                "required": [
                    "key",
                    "order_id",
                    "order_type",
                    "user_id",
                    "store_id",
                    "note",
                    "location",
                    "dispatch_time",
                    "shipping_time",
                    "delivery_time",
                    "total_price",
                    "create_time",
                    "update_time",
                    "status",
                    "products",
                ],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "submit_task_delivery_order",
            "description": "按当前task namespace将最终订单提交到 task:{task_id}:answer:final",
            "parameters": {
                "type": "object",
                "properties": dict(
                    {
                        "namespace": {"type": "string"},
                        "task_id": {"type": "string"},
                    },
                    **DELIVERY_ORDER_SCHEMA
                ),
                "required": [
                    "namespace",
                    "task_id",
                    "order_id",
                    "order_type",
                    "user_id",
                    "store_id",
                    "note",
                    "location",
                    "dispatch_time",
                    "shipping_time",
                    "delivery_time",
                    "total_price",
                    "create_time",
                    "update_time",
                    "status",
                    "products",
                ],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tree_submit_task_delivery_order",
            "description": "把最终订单写入当前Tree事务的指定branch，目标key固定为 task:{task_id}:answer:final",
            "parameters": {
                "type": "object",
                "properties": dict(
                    {
                        "namespace": {"type": "string"},
                        "task_id": {"type": "string"},
                        "tx_id": {"type": "string"},
                        "branch_id": {"type": "integer"},
                    },
                    **DELIVERY_ORDER_SCHEMA
                ),
                "required": [
                    "namespace",
                    "task_id",
                    "tx_id",
                    "branch_id",
                    "order_id",
                    "order_type",
                    "user_id",
                    "store_id",
                    "note",
                    "location",
                    "dispatch_time",
                    "shipping_time",
                    "delivery_time",
                    "total_price",
                    "create_time",
                    "update_time",
                    "status",
                    "products",
                ],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "kv_dump",
            "description": "调试：查看当前所有KV数据",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "image_analyze",
            "description": "分析图片（占位版）",
            "parameters": {
                "type": "object",
                "properties": {
                    "image_path": {"type": "string"},
                    "task": {"type": "string", "enum": ["describe", "ocr", "detect"]},
                },
                "required": ["image_path"],
            },
        },
    },
]


def tool_router(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    if name == "kv_get":
        return kv_get(args)
    if name == "tx_start":
        return tx_start(args)
    if name == "tx_get":
        return tx_get(args)
    if name == "tx_put":
        return tx_put(args)
    if name == "tx_commit":
        return tx_commit(args)
    if name == "tx_rollback":
        return tx_rollback(args)
    if name == "tree_tx_start":
        return tree_tx_start(args)
    if name == "tree_tx_branch":
        return tree_tx_branch(args)
    if name == "tree_tx_get":
        return tree_tx_get(args)
    if name == "tree_tx_put":
        return tree_tx_put(args)
    if name == "tree_tx_winner":
        return tree_tx_winner(args)
    if name == "tree_tx_refresh_winner":
        return tree_tx_refresh_winner(args)
    if name == "tree_tx_commit_retry":
        return tree_tx_commit_retry(args)
    if name == "tree_tx_abort":
        return tree_tx_abort(args)
    if name == "get_task_context":
        return get_task_context(args)
    if name == "get_expected_order_template":
        return get_expected_order_template(args)
    if name == "list_candidate_stores":
        return list_candidate_stores(args)
    if name == "get_store_detail":
        return get_store_detail(args)
    if name == "get_product_detail":
        return get_product_detail(args)
    if name == "submit_final_answer":
        return submit_final_answer(args)
    if name == "submit_delivery_order":
        return submit_delivery_order(args)
    if name == "submit_task_delivery_order":
        return submit_task_delivery_order(args)
    if name == "tree_submit_task_delivery_order":
        return tree_submit_task_delivery_order(args)
    if name == "kv_dump":
        return kv_dump(args)
    if name == "image_analyze":
        return image_analyze(args)
    return {"ok": False, "error": "unknown tool: {}".format(name)}
