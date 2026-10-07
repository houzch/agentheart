"use strict";
/**
 * AgentHeart Node/TS SDK：内核接口客户端（仅标准库 net）。
 *
 * 协议：16 字节小端帧头 + UTF-8 JSON 载荷（见方案第 8.3 节）。
 *
 * 特性：
 * - **单连接事件流**：后台解析数据帧，事件帧入队（`drainEvents`），响应帧按 requestId 分发；
 * - **类型化便捷方法**：任务 / 定时 / 队列 / 循环 / 链路 / 指标；
 * - **断线重连**：`reconnect()` 重建连接并重新握手（连接级状态独立，避免旧读线程污染）。
 */

const net = require("net");

const MAGIC = 0x41480001;
const PROTOCOL_VERSION = 1;
const HEADER_LEN = 16;
const FLAG_REQUEST = 0x1;
const FLAG_EVENT = 0x4;
const MAX_EVENTS = 1024;
const DEFAULT_TIMEOUT = 5000;

function encodeFrame(requestId, payload) {
  const body = Buffer.from(payload, "utf8");
  const header = Buffer.alloc(HEADER_LEN);
  header.writeUInt32LE(MAGIC, 0);
  header.writeUInt8(PROTOCOL_VERSION, 4);
  header.writeUInt8(0, 5);
  header.writeUInt16LE(FLAG_REQUEST, 6);
  header.writeUInt32LE(requestId, 8);
  header.writeUInt32LE(body.length, 12);
  return Buffer.concat([header, body]);
}

function parseAddress(addr) {
  const index = String(addr).lastIndexOf(":");
  if (index <= 0) throw new Error(`地址非法: ${addr}`);
  return { host: addr.slice(0, index), port: Number(addr.slice(index + 1)) };
}

/** 单连接的解析状态（重连时整体替换，互不干扰）。 */
class Connection {
  constructor(socket, onClose) {
    this.socket = socket;
    this.buffer = Buffer.alloc(0);
    this.pending = new Map();
    this.events = [];
    this.closed = false;
    this.onClose = onClose;
    socket.on("data", (chunk) => this.onData(chunk));
    socket.on("error", () => this.fail());
    socket.on("close", () => this.fail());
  }

  onData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    while (this.buffer.length >= HEADER_LEN) {
      if (this.buffer.readUInt32LE(0) !== MAGIC) {
        this.fail();
        return;
      }
      const flags = this.buffer.readUInt16LE(6);
      const requestId = this.buffer.readUInt32LE(8);
      const length = this.buffer.readUInt32LE(12);
      if (this.buffer.length < HEADER_LEN + length) return;
      const body = this.buffer.subarray(HEADER_LEN, HEADER_LEN + length).toString("utf8");
      this.buffer = this.buffer.subarray(HEADER_LEN + length);
      let value;
      try {
        value = JSON.parse(body);
      } catch {
        continue;
      }
      if (flags & FLAG_EVENT) {
        this.events.push(value);
        while (this.events.length > MAX_EVENTS) this.events.shift();
      } else {
        const waiter = this.pending.get(requestId);
        if (waiter) {
          this.pending.delete(requestId);
          waiter.resolve(value);
        }
      }
    }
  }

  fail() {
    if (this.closed) return;
    this.closed = true;
    for (const waiter of this.pending.values()) {
      waiter.reject(new Error("内核连接已关闭"));
    }
    this.pending.clear();
    if (this.onClose) this.onClose();
  }

  write(frame) {
    return new Promise((resolve, reject) => {
      this.socket.write(frame, (error) => (error ? reject(error) : resolve()));
    });
  }
}

class Client {
  constructor() {
    this.connection = null;
    this.nextId = 1;
    this.address = null;
    this.token = "";
    this.timeout = DEFAULT_TIMEOUT;
  }

  /** 连接并完成握手。 */
  static async connect(addr, token = "", timeout = DEFAULT_TIMEOUT) {
    const client = new Client();
    client.address = addr;
    client.token = token;
    client.timeout = timeout;
    await client.reconnect();
    return client;
  }

  /** 建立底层连接并握手（亦用于断线重连）。 */
  async reconnect() {
    const { host, port } = parseAddress(this.address);
    const socket = await new Promise((resolve, reject) => {
      const connecting = net.connect({ host, port }, () => resolve(connecting));
      connecting.once("error", reject);
    });
    socket.setNoDelay(true);
    // 连接级状态整体替换：旧连接的读状态不再影响新连接
    this.nextId = 1;
    this.connection = new Connection(socket, null);

    const response = await this.callRaw(
      `{"m":"system.hello","ver":${PROTOCOL_VERSION},"token":"${this.token}"}`
    );
    if (!response.ok) throw new Error("agentheart: 握手被拒绝");
  }

  /** 发送一次请求，返回解析后的响应对象。 */
  call(request) {
    const body = typeof request === "string" ? request : JSON.stringify(request);
    return this.callRaw(body);
  }

  /** 发送一次请求，返回响应原文（JSON 字符串）对应的对象。 */
  async callRaw(request) {
    const connection = this.connection;
    if (!connection || connection.closed) throw new Error("agentheart: 未连接或连接已关闭");
    const requestId = this.nextId++;
    const answer = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        if (connection.pending.delete(requestId)) {
          reject(new Error(`agentheart: 请求超时（${requestId}）`));
        }
      }, this.timeout);
      connection.pending.set(requestId, {
        // 清理超时定时器，避免拖住事件循环与进程退出
        resolve: (value) => {
          clearTimeout(timer);
          resolve(value);
        },
        reject: (error) => {
          clearTimeout(timer);
          reject(error);
        },
      });
    });
    await connection.write(encodeFrame(requestId, request));
    return answer;
  }

  /** 订阅事件流（`topics` 为空或含 `*` 表示全部）。 */
  subscribe(topics, fromSeq = 0) {
    return this.call({ m: "stream.subscribe", topics, fromSeq });
  }

  /** 取走自上次调用以来收到的事件（非阻塞）。 */
  drainEvents() {
    const connection = this.connection;
    if (!connection) return [];
    const events = connection.events;
    connection.events = [];
    return events;
  }

  close() {
    if (this.connection && this.connection.socket) {
      this.connection.socket.destroy();
      this.connection.closed = true;
    }
  }

  // ---- 类型化便捷方法 ----

  health() { return this.call({ m: "system.health" }); }
  heartbeat() { return this.call({ m: "heartbeat.get" }); }
  metrics() { return this.call({ m: "metrics.get" }); }
  trace(taskId) { return this.call({ m: "trace.get", taskId }); }

  submitTask(queue, name) {
    const request = { m: "task.trigger", queue };
    if (name) request.name = name;
    return this.call(request);
  }
  getTask(taskId) { return this.call({ m: "task.get", taskId }); }
  pauseTask(taskId) { return this.call({ m: "task.pause", taskId }); }
  resumeTask(taskId) { return this.call({ m: "task.resume", taskId }); }
  retryTask(taskId, resetAttempts = false) {
    return this.call({ m: "task.retry", taskId, resetAttempts });
  }
  cancelTask(taskId) { return this.call({ m: "task.cancel", taskId }); }

  /** 游标分页：返回 `{ items, nextCursor }`。 */
  async listTasks(limit = 50, cursor) {
    const page = { limit };
    if (cursor) page.cursor = cursor;
    const response = await this.call({ m: "task.list", page });
    const result = response.result || {};
    return { items: result.items || [], nextCursor: result.nextCursor || null };
  }

  jobs() { return this.call({ m: "job.list" }); }
  queues() { return this.call({ m: "queue.list" }); }
  queueStats(queue) { return this.call({ m: "queue.stats", queue }); }
  declareQueue(queue, capacity) {
    const request = { m: "queue.declare", queue };
    if (capacity) request.capacity = capacity;
    return this.call(request);
  }
  publish(queue, body) { return this.call({ m: "queue.publish", queue, body }); }

  loops() { return this.call({ m: "loop.list" }); }
  createLoop(name, maxIterations, intervalMs = 0) {
    const request = { m: "loop.create", maxIterations, intervalMs };
    if (name) request.name = name;
    return this.call(request);
  }
  loopControl(action, loopId) { return this.call({ m: `loop.${action}`, loopId }); }
}

module.exports = { Client, encodeFrame, MAGIC, PROTOCOL_VERSION, HEADER_LEN };
