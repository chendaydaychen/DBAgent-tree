# Agent/llm.py
import copy
import json
import os
from typing import Any, Dict, List, Optional

from openai import BadRequestError, OpenAI

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv() -> bool:
        return False


load_dotenv()

# Prefer the repo-level OPENAI_MODEL convention and keep MODEL_NAME as fallback
# for compatibility with older local setups.
MODEL_NAME = os.getenv("OPENAI_MODEL") or os.getenv("MODEL_NAME", "qwen-plus")
TOOL_MODE = (os.getenv("OPENAI_TOOL_MODE") or "auto").strip().lower()
DEFAULT_TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE") or "0")
_client = None  # type: Optional[OpenAI]


def _create_kwargs(messages: List[Dict[str, Any]], **extra: Any) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {
        "model": MODEL_NAME,
        "messages": messages,
        **extra,
    }
    seed = (os.getenv("OPENAI_SEED") or "").strip()
    if seed:
        try:
            kwargs["seed"] = int(seed)
        except ValueError:
            pass
    deepseek_thinking = (os.getenv("DEEPSEEK_THINKING") or "").strip().lower()
    base_url = os.getenv("OPENAI_BASE_URL", "")
    if "api.deepseek.com" in base_url and deepseek_thinking in {"disabled", "off", "false", "0"}:
        kwargs["extra_body"] = {"thinking": {"type": "disabled"}}
    return kwargs


def _get_client() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI(
            api_key=os.getenv("OPENAI_API_KEY"),
            base_url=os.getenv("OPENAI_BASE_URL"),
        )
    return _client


def _tools_summary(tools: List[Dict[str, Any]]) -> str:
    rows: List[str] = []
    for tool in tools:
        fn = tool.get("function", {})
        rows.append(json.dumps({
            "name": fn.get("name"),
            "description": fn.get("description"),
            "parameters": fn.get("parameters", {}),
        }, ensure_ascii=False))
    return "\n".join(rows)


def _extract_json_blob(text: str) -> Dict[str, Any]:
    raw = (text or "").strip()
    if raw.startswith("```"):
        lines = raw.splitlines()
        if len(lines) >= 3:
            raw = "\n".join(lines[1:-1]).strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end >= start:
        raw = raw[start:end + 1]
    return json.loads(raw)


def _extract_action_payloads(text: str) -> List[Dict[str, Any]]:
    try:
        return [_extract_json_blob(text)]
    except Exception:
        pass

    payloads: List[Dict[str, Any]] = []
    for line in (text or "").splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        if candidate.startswith("```") or candidate.endswith("```"):
            continue
        try:
            payload = _extract_json_blob(candidate)
        except Exception:
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
    if payloads:
        return payloads
    raise ValueError("no valid action payload found")


def _manual_tool_messages(messages: List[Dict[str, Any]], tools: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    manual_system = (
        "你正在一个不支持原生 tool calling 的 OpenAI-compatible 接口上运行。"
        "你仍然必须通过工具完成读写。"
        "可用工具如下，每次只能调用一个：\n"
        f"{_tools_summary(tools)}\n\n"
        "你的回复必须是 JSON，且不能加 markdown 代码块。\n"
        "如果要调用工具，输出："
        '{"action":"tool","name":"工具名","arguments":{"参数":"值"}}\n'
        "如果任务已经完成，输出："
        '{"action":"final","answer":"给用户的最终中文答复"}'
    )
    converted: List[Dict[str, str]] = [{"role": "system", "content": manual_system}]
    for msg in messages:
        role = msg.get("role", "user")
        if role in {"system", "user", "assistant"}:
            converted.append({"role": role, "content": msg.get("content", "")})
    return converted


def _chat_with_tools_manual(
    messages: List[Dict[str, Any]],
    tools: List[Dict[str, Any]],
    tool_router,
    max_rounds: int = 10,
    return_messages: bool = False,
) -> str:
    manual_messages = _manual_tool_messages(messages, tools)

    for _ in range(max_rounds):
        resp = _get_client().chat.completions.create(
            **_create_kwargs(manual_messages, temperature=DEFAULT_TEMPERATURE)
        )
        content = resp.choices[0].message.content or ""

        try:
            payloads = _extract_action_payloads(content)
        except Exception:
            answer = content.strip() or "模型未返回可解析结果。"
            if return_messages:
                return {"answer": answer, "messages": manual_messages, "tool_mode": "manual"}
            return answer
        tool_results: List[Dict[str, Any]] = []
        for payload in payloads:
            action = str(payload.get("action", "")).strip().lower()
            if action == "tool":
                fn_name = str(payload.get("name", "")).strip()
                fn_args = payload.get("arguments", {})
                if not isinstance(fn_args, dict):
                    fn_args = {}
                result = tool_router(fn_name, fn_args)
                tool_results.append({
                    "name": fn_name,
                    "arguments": fn_args,
                    "result": result,
                })
                continue
            if action and action not in {"final"} and "arguments" in payload:
                fn_name = str(payload.get("name") or payload.get("action") or "").strip()
                fn_args = payload.get("arguments", {})
                if not isinstance(fn_args, dict):
                    fn_args = {}
                result = tool_router(fn_name, fn_args)
                tool_results.append({
                    "name": fn_name,
                    "arguments": fn_args,
                    "result": result,
                })
                continue
            if action == "final":
                answer = payload.get("answer", "")
                final_answer = str(answer).strip() or "任务已完成。"
                if return_messages:
                    manual_messages.append({"role": "assistant", "content": content})
                    return {"answer": final_answer, "messages": manual_messages, "tool_mode": "manual"}
                return final_answer
            fallback = content.strip() or "模型未返回可执行动作。"
            if return_messages:
                return {"answer": fallback, "messages": manual_messages, "tool_mode": "manual"}
            return fallback

        if tool_results:
            manual_messages.append({
                "role": "assistant",
                "content": "\n".join(json.dumps(item, ensure_ascii=False) for item in payloads),
            })
            manual_messages.append({
                "role": "user",
                "content": "工具执行结果如下，请继续决策：{}".format(
                    json.dumps(tool_results, ensure_ascii=False)
                ),
            })
            continue

        answer = content.strip() or "模型未返回可执行动作。"
        if return_messages:
            return {"answer": answer, "messages": manual_messages, "tool_mode": "manual"}
        return answer

    answer = "达到最大工具调用轮次，流程未完成。"
    if return_messages:
        return {"answer": answer, "messages": manual_messages, "tool_mode": "manual"}
    return answer


def _chat_with_tools_native(
    messages: List[Dict[str, Any]],
    tools: List[Dict[str, Any]],
    tool_router,
    max_rounds: int = 10,
    return_messages: bool = False,
) -> str:
    for _ in range(max_rounds):
        resp = _get_client().chat.completions.create(
            **_create_kwargs(
                messages,
                tools=tools,
                tool_choice="auto",
                temperature=DEFAULT_TEMPERATURE,
            )
        )

        msg = resp.choices[0].message

        if msg.tool_calls:
            messages.append({
                "role": "assistant",
                "content": msg.content or "",
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": tc.type,
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        },
                    } for tc in msg.tool_calls
                ],
            })

            for tc in msg.tool_calls:
                fn_name = tc.function.name
                raw_args = tc.function.arguments or "{}"

                try:
                    fn_args = json.loads(raw_args)
                except Exception:
                    fn_args = {}

                result = tool_router(fn_name, fn_args)

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "name": fn_name,
                    "content": json.dumps(result, ensure_ascii=False),
                })
            continue

        answer = msg.content or ""
        if return_messages:
            return {"answer": answer, "messages": messages, "tool_mode": "native"}
        return answer

    answer = "达到最大工具调用轮次，流程未完成。"
    if return_messages:
        return {"answer": answer, "messages": messages, "tool_mode": "native"}
    return answer


def chat_with_tools(
    messages: List[Dict[str, Any]],
    tools: List[Dict[str, Any]],
    tool_router,
    max_rounds: int = 10,
) -> str:
    if TOOL_MODE == "manual":
        return _chat_with_tools_manual(copy.deepcopy(messages), tools, tool_router, max_rounds=max_rounds)

    if TOOL_MODE == "native":
        return _chat_with_tools_native(messages, tools, tool_router, max_rounds=max_rounds)

    try:
        return _chat_with_tools_native(messages, tools, tool_router, max_rounds=max_rounds)
    except BadRequestError as exc:
        if "tool choice" not in str(exc).lower() and "tool" not in str(exc).lower():
            raise
        return _chat_with_tools_manual(copy.deepcopy(messages), tools, tool_router, max_rounds=max_rounds)


def chat_with_tools_session(
    messages: List[Dict[str, Any]],
    tools: List[Dict[str, Any]],
    tool_router,
    max_rounds: int = 10,
) -> Dict[str, Any]:
    if TOOL_MODE == "manual":
        return _chat_with_tools_manual(messages, tools, tool_router, max_rounds=max_rounds, return_messages=True)

    if TOOL_MODE == "native":
        return _chat_with_tools_native(messages, tools, tool_router, max_rounds=max_rounds, return_messages=True)

    try:
        return _chat_with_tools_native(messages, tools, tool_router, max_rounds=max_rounds, return_messages=True)
    except BadRequestError as exc:
        if "tool choice" not in str(exc).lower() and "tool" not in str(exc).lower():
            raise
        return _chat_with_tools_manual(messages, tools, tool_router, max_rounds=max_rounds, return_messages=True)
