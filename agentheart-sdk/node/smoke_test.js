"use strict";
/**
 * AgentHeart Node SDK 冒烟测试（M10）
 *
 * 需先启动侧车（公开仓库可用 `scripts/quickstart.ps1`，或手动运行
 * `target/release/agentheartd`），再读取环境变量
 * `AH_ADDR`（host:port）与 `AH_TOKEN`，完成：
 * 握手 → 健康 → 提交/查询任务 → 游标分页 → 事件订阅 → 队列 → 断线重连。
 *
 * 全部通过时打印 `SMOKE_OK node` 并以 0 退出。
 */

const assert = require("assert");
const { Client } = require("./agentheart");

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitFor(describe, probe, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await probe()) return;
    await sleep(30);
  }
  throw new Error(`等待超时: ${describe}`);
}

function taskIdOf(response) {
  const id = response && response.result && response.result.taskId;
  assert.ok(id, `响应缺少 taskId: ${JSON.stringify(response)}`);
  return id;
}

async function main() {
  const addr = process.env.AH_ADDR;
  const token = process.env.AH_TOKEN || "";
  assert.ok(addr, "缺少环境变量 AH_ADDR");

  const client = await Client.connect(addr, token);
  console.log("步骤 1 连接与握手：通过");

  const health = await client.health();
  assert.strictEqual(health.ok, true, "system.health 失败");
  console.log(`步骤 2 健康检查：通过（status=${health.result.status}）`);

  const submitted = await client.submitTask("e2e", "smoke");
  const taskId = taskIdOf(submitted);
  await waitFor("任务完成", async () => {
    const response = await client.getTask(taskId);
    return response.result.task.state === "succeeded";
  });
  console.log(`步骤 3/4 提交并查询任务：通过（${taskId} → succeeded）`);

  // 游标分页：每页 1 条，翻页累计
  for (let index = 0; index < 3; index += 1) {
    await client.submitTask("e2e", `page-${index}`);
  }
  let cursor = null;
  let total = 0;
  let pages = 0;
  do {
    const page = await client.listTasks(1, cursor);
    total += page.items.length;
    cursor = page.nextCursor;
    pages += 1;
    assert.ok(pages <= 50, "分页未收敛");
  } while (cursor);
  assert.ok(total >= 3, `分页累计不足：${total}`);
  console.log(`步骤 5 游标分页：通过（${pages} 页，累计 ${total} 条）`);

  // 事件订阅
  const subscribed = await client.subscribe(["task"], 0);
  assert.strictEqual(subscribed.ok, true, "stream.subscribe 失败");
  await client.submitTask("e2e", "event-probe");
  await waitFor("event.task/succeeded", async () =>
    client
      .drainEvents()
      .some((event) => event.m === "event.task" && event.state === "succeeded")
  );
  console.log("步骤 6 事件订阅：通过（收到 event.task · succeeded）");

  // 队列
  const declared = await client.declareQueue("e2e-q", 5);
  assert.strictEqual(declared.ok, true, "queue.declare 失败");
  const published = await client.publish("e2e-q", "hi");
  assert.strictEqual(published.ok, true, "queue.publish 失败");
  const stats = await client.queueStats("e2e-q");
  assert.ok(stats.result.queues[0].depth >= 1, `队列深度异常: ${JSON.stringify(stats)}`);
  console.log(`步骤 7 队列：通过（depth=${stats.result.queues[0].depth}）`);

  // 断线重连
  client.close();
  await client.reconnect();
  const again = await client.health();
  assert.strictEqual(again.ok, true, "重连后 health 失败");
  console.log("步骤 8 断线重连：通过");
  client.close();

  console.log("SMOKE_OK node");
}

main().catch((error) => {
  console.error(`SMOKE_FAILED node: ${error.message}`);
  process.exit(1);
});
