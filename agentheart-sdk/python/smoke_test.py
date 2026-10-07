"""AgentHeart Python SDK 冒烟测试（M10）。

需先启动侧车（公开仓库可用 ``scripts/quickstart.ps1``，或手动运行
``target/release/agentheartd``），再读取环境变量
``AH_ADDR``（host:port）与 ``AH_TOKEN``，完成：
握手 → 健康 → 提交/查询任务 → 游标分页 → 事件订阅 → 队列 → 断线重连。

全部通过时打印 ``SMOKE_OK python`` 并以 0 退出。
"""

from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agentheart_client import Client  # noqa: E402


def wait_for(describe: str, probe, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if probe():
            return
        time.sleep(0.03)
    raise AssertionError(f"等待超时: {describe}")


def main() -> None:
    address = os.environ.get("AH_ADDR")
    token = os.environ.get("AH_TOKEN", "")
    if not address:
        raise AssertionError("缺少环境变量 AH_ADDR")

    client = Client.connect(address, token)
    print("步骤 1 连接与握手：通过")

    health = client.health()
    assert health.get("ok") is True, "system.health 失败"
    print(f"步骤 2 健康检查：通过（status={health['result']['status']}）")

    task_id = client.submit_task("e2e", "smoke")["result"]["taskId"]

    def finished() -> bool:
        return client.get_task(task_id)["result"]["task"]["state"] == "succeeded"

    wait_for("任务完成", finished)
    print(f"步骤 3/4 提交并查询任务：通过（{task_id} → succeeded）")

    # 游标分页：每页 1 条，翻页累计
    for index in range(3):
        client.submit_task("e2e", f"page-{index}")
    cursor = None
    total = 0
    pages = 0
    while True:
        page = client.list_tasks(1, cursor)
        total += len(page["items"])
        cursor = page["nextCursor"]
        pages += 1
        assert pages <= 50, "分页未收敛"
        if not cursor:
            break
    assert total >= 3, f"分页累计不足：{total}"
    print(f"步骤 5 游标分页：通过（{pages} 页，累计 {total} 条）")

    # 事件订阅
    assert client.subscribe(["task"], 0).get("ok") is True, "stream.subscribe 失败"
    client.submit_task("e2e", "event-probe")

    def got_event() -> bool:
        return any(
            event.get("m") == "event.task" and event.get("state") == "succeeded"
            for event in client.drain_events()
        )

    wait_for("event.task/succeeded", got_event)
    print("步骤 6 事件订阅：通过（收到 event.task · succeeded）")

    # 队列
    assert client.declare_queue("e2e-q", 5).get("ok") is True, "queue.declare 失败"
    assert client.publish("e2e-q", "hi").get("ok") is True, "queue.publish 失败"
    depth = client.queue_stats("e2e-q")["result"]["queues"][0]["depth"]
    assert depth >= 1, f"队列深度异常: {depth}"
    print(f"步骤 7 队列：通过（depth={depth}）")

    # 断线重连
    client.close()
    client.reconnect()
    assert client.health().get("ok") is True, "重连后 health 失败"
    print("步骤 8 断线重连：通过")
    client.close()

    print("SMOKE_OK python")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # noqa: BLE001 - 冒烟测试统一兜底输出
        print(f"SMOKE_FAILED python: {error}", file=sys.stderr)
        sys.exit(1)
