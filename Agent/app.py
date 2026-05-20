# Agent/app.py
from llm import chat_with_tools
from tool_registry import TOOLS_SCHEMA, tool_router

SYSTEM_PROMPT = """你是一个事务安全的多模态数据助理。

你可以使用这些工具：
1) KV工具：kv_get
2) 事务工具：tx_start, tx_get, tx_put, tx_commit, tx_rollback
3) 图片工具：image_analyze
4) 调试工具：kv_dump

强约束规则：
- 任何写入必须走事务：先 tx_start，再 tx_put，最后 tx_commit。
- 任一步失败，必须调用 tx_rollback。
- 读取可使用 kv_get 或 tx_get。
- 涉及图片理解时，必须调用 image_analyze。
- 最终回复必须说明：执行了哪些工具、是否提交/回滚、最终结果是什么。
- 全部使用中文回答，简洁清晰。
"""


def main():
    print("DataAgent 启动成功。输入 quit/exit 退出。")
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]

    while True:
        user_input = input("\n你> ").strip()
        if user_input.lower() in {"quit", "exit"}:
            print("已退出。")
            break

        messages.append({"role": "user", "content": user_input})
        answer = chat_with_tools(messages, TOOLS_SCHEMA, tool_router)
        messages.append({"role": "assistant", "content": answer})

        print(f"\n助手> {answer}")


if __name__ == "__main__":
    main()
