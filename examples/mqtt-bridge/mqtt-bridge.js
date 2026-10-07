// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

"use strict";
/**
 * MQTT 桥接示例：外部标准 MQTT 客户端 ↔ AgentHeart 内核。
 *
 * 仅使用 Node 标准库（net / path），CommonJS。自行实现 MQTT 3.1.1 子集客户端，
 * 字节格式与内核端 `protocol/mqtt/packet.rs` 保持一致。
 *
 * 环境变量：
 * - `AH_MQTT_ADDR`：侧车 MQTT 端口，形如 `127.0.0.1:1883`；
 * - `AH_ADDR`      ：帧协议端口，形如 `127.0.0.1:xxxxx`；
 * - `AH_TOKEN`     ：帧协议访问令牌（MQTT 端口无鉴权，不校验）。
 *
 * 全部步骤成功打印 `MQTT_BRIDGE_OK` 并以 0 退出，否则打印错误并以 1 退出。
 */

const net = require("net");
const path = require("path");

// 单次读取/等待的超时（毫秒）
const READ_TIMEOUT = 5000;

// MQTT 报文类型（固定头高 4 位）
const TYPE = {
  CONNECT: 1,
  CONNACK: 2,
  PUBLISH: 3,
  PUBACK: 4,
  SUBSCRIBE: 8,
  SUBACK: 9,
  UNSUBSCRIBE: 10,
  UNSUBACK: 11,
  PINGREQ: 12,
  PINGRESP: 13,
  DISCONNECT: 14,
};

/** 解析 `host:port` 形式的地址。 */
function parseAddress(addr) {
  const index = String(addr).lastIndexOf(":");
  if (index <= 0) throw new Error(`地址非法: ${addr}`);
  return { host: addr.slice(0, index), port: Number(addr.slice(index + 1)) };
}

/** 编码 MQTT 变长剩余长度（每字节 7 位，最高位为续接标志，最多 4 字节）。 */
function writeVarint(value) {
  const out = [];
  let v = value;
  do {
    let byte = v % 128;
    v = Math.floor(v / 128);
    if (v > 0) byte |= 0x80;
    out.push(byte);
  } while (v > 0);
  return Buffer.from(out);
}

/** 组包：固定头 + 变长长度 + 载荷。`kind` 为报文类型，`flags` 为低 4 位标志。 */
function packet(kind, flags, body) {
  const payload = Buffer.isBuffer(body) ? body : Buffer.from(body || []);
  const head = Buffer.from([((kind << 4) | (flags & 0x0f)) & 0xff]);
  return Buffer.concat([head, writeVarint(payload.length), payload]);
}

/** 向分片数组追加 MQTT 字符串：2 字节大端长度 + UTF-8 内容。 */
function pushString(parts, text) {
  const buf = Buffer.from(text, "utf8");
  const len = Buffer.alloc(2);
  len.writeUInt16BE(buf.length, 0);
  parts.push(len, buf);
}

/** CONNECT：协议名 "MQTT"、级别 4、clean session。 */
function connectPacket(clientId, keepAlive) {
  const parts = [];
  pushString(parts, "MQTT");
  parts.push(Buffer.from([4, 0x02])); // 协议级别 4；连接标志 0x02 = clean session
  const ka = Buffer.alloc(2);
  ka.writeUInt16BE(keepAlive, 0);
  parts.push(ka);
  pushString(parts, clientId);
  return packet(TYPE.CONNECT, 0, Buffer.concat(parts));
}

/** SUBSCRIBE：固定头标志必须为 0x02。`filters` 为 [filter, qos] 数组。 */
function subscribePacket(packetId, filters) {
  const parts = [];
  const pid = Buffer.alloc(2);
  pid.writeUInt16BE(packetId, 0);
  parts.push(pid);
  for (const [filter, qos] of filters) {
    pushString(parts, filter);
    parts.push(Buffer.from([qos & 0x03]));
  }
  return packet(TYPE.SUBSCRIBE, 0x02, Buffer.concat(parts));
}

/** PUBLISH：QoS 0 时无包 ID；QoS 1 时携带包 ID。 */
function publishPacket(topic, payload, qos = 0, packetId = 0) {
  const parts = [];
  pushString(parts, topic);
  if (qos > 0) {
    const pid = Buffer.alloc(2);
    pid.writeUInt16BE(packetId, 0);
    parts.push(pid);
  }
  parts.push(Buffer.isBuffer(payload) ? payload : Buffer.from(String(payload), "utf8"));
  return packet(TYPE.PUBLISH, (qos & 0x03) << 1, Buffer.concat(parts));
}

/** 解析 PUBLISH 载荷，返回 { qos, topic, payload }。 */
function parsePublish(flags, body) {
  const qos = (flags >> 1) & 0x03;
  let pos = 0;
  const topicLen = body.readUInt16BE(pos);
  pos += 2;
  const topic = body.subarray(pos, pos + topicLen).toString("utf8");
  pos += topicLen;
  if (qos > 0) pos += 2; // 跳过包 ID
  return { qos, topic, payload: body.subarray(pos).toString("utf8") };
}

/** 最小 MQTT 客户端：TCP 字节流解析 + 报文队列 + 异步读取。 */
class MqttClient {
  constructor(socket) {
    this.socket = socket;
    this.buffer = Buffer.alloc(0);
    this.pending = [];
    this.waiter = null;
    this.closed = false;
    this.closeError = null;
    socket.on("data", (chunk) => {
      this.buffer = Buffer.concat([this.buffer, chunk]);
      this.parse();
    });
    socket.on("error", (err) => this.fail(err));
    socket.on("close", () => this.fail(new Error("MQTT 连接已关闭")));
  }

  /** 从缓冲区中尽可能多地切出完整报文。 */
  parse() {
    for (;;) {
      if (this.buffer.length < 2) return;
      let value = 0;
      let multiplier = 1;
      let index = 1;
      let complete = false;
      for (let i = 0; i < 4 && index < this.buffer.length; i += 1) {
        const byte = this.buffer[index];
        index += 1;
        value += (byte & 0x7f) * multiplier;
        multiplier *= 128;
        if ((byte & 0x80) === 0) {
          complete = true;
          break;
        }
      }
      if (!complete) return;
      const total = index + value;
      if (this.buffer.length < total) return;
      const type = this.buffer[0] >> 4;
      const flags = this.buffer[0] & 0x0f;
      const body = this.buffer.subarray(index, total);
      this.buffer = this.buffer.subarray(total);
      this.deliver({ type, flags, payload: body });
    }
  }

  deliver(pkt) {
    if (this.waiter) {
      const waiter = this.waiter;
      this.waiter = null;
      waiter.resolve(pkt);
    } else {
      this.pending.push(pkt);
    }
  }

  fail(err) {
    if (this.closed) return;
    this.closed = true;
    this.closeError = err;
    if (this.waiter) {
      const waiter = this.waiter;
      this.waiter = null;
      waiter.reject(err);
    }
  }

  /** 读取下一个报文，超时（毫秒）抛错。 */
  read(timeoutMs) {
    if (this.pending.length > 0) return Promise.resolve(this.pending.shift());
    if (this.closed) return Promise.reject(this.closeError || new Error("MQTT 连接已关闭"));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.waiter && this.waiter.timer === timer) this.waiter = null;
        reject(new Error(`读取 MQTT 报文超时（${timeoutMs}ms）`));
      }, Math.max(1, timeoutMs));
      this.waiter = {
        timer,
        resolve: (pkt) => {
          clearTimeout(timer);
          resolve(pkt);
        },
        reject: (err) => {
          clearTimeout(timer);
          reject(err);
        },
      };
    });
  }

  write(buf) {
    return new Promise((resolve, reject) => {
      this.socket.write(buf, (err) => (err ? reject(err) : resolve()));
    });
  }

  close() {
    try {
      this.socket.end();
    } catch {
      /* 忽略关闭异常 */
    }
  }
}

/** 建立 TCP 连接并返回 MqttClient。 */
function connectMqtt(addr, clientId, keepAlive = 60) {
  return new Promise((resolve, reject) => {
    const { host, port } = parseAddress(addr);
    const socket = net.connect({ host, port }, async () => {
      socket.setNoDelay(true);
      const client = new MqttClient(socket);
      try {
        await client.write(connectPacket(clientId, keepAlive));
        resolve(client);
      } catch (err) {
        client.close();
        reject(err);
      }
    });
    socket.once("error", reject);
  });
}

/**
 * 读取直到出现指定类型的报文；其余报文暂存（保持顺序）以便后续读取。
 */
async function readUntilType(client, type, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  const other = [];
  try {
    for (;;) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw new Error(`等待 MQTT 报文类型 ${type} 超时`);
      const pkt = await client.read(remaining);
      if (pkt.type === type) return pkt;
      other.push(pkt);
    }
  } finally {
    for (let i = other.length - 1; i >= 0; i -= 1) client.pending.unshift(other[i]);
  }
}

/** 打印一行并等待 stdout 冲刷完成。 */
function writeLine(text) {
  return new Promise((resolve) => process.stdout.write(`${text}\n`, resolve));
}

async function main() {
  const mqttAddr = process.env.AH_MQTT_ADDR;
  const ahAddr = process.env.AH_ADDR;
  const ahToken = process.env.AH_TOKEN || "";
  if (!mqttAddr) throw new Error("缺少环境变量 AH_MQTT_ADDR");
  if (!ahAddr) throw new Error("缺少环境变量 AH_ADDR");

  const { Client } = require(path.join(__dirname, "..", "..", "agentheart-sdk", "node", "agentheart.js"));

  let ah = null;
  let mqtt = null;
  try {
    // 帧协议客户端：用于验证消息确实进入内核队列
    ah = await Client.connect(ahAddr, ahToken, READ_TIMEOUT);

    // ---- 步骤 1：MQTT 连接，期望 CONNACK 返回码 0 ----
    mqtt = await connectMqtt(mqttAddr, "mqtt-bridge-demo");
    const connack = await readUntilType(mqtt, TYPE.CONNACK, READ_TIMEOUT);
    if (connack.payload.length < 2 || connack.payload[1] !== 0) {
      throw new Error(`CONNACK 返回码非 0：${connack.payload.toString("hex")}`);
    }
    await writeLine("步骤1 完成：MQTT 已连接，CONNACK 返回码 0");

    // ---- 步骤 2：订阅 bridge/#，期望授予 QoS 0 ----
    await mqtt.write(subscribePacket(1, [["bridge/#", 0]]));
    const suback = await readUntilType(mqtt, TYPE.SUBACK, READ_TIMEOUT);
    const subPid = suback.payload.readUInt16BE(0);
    const granted = suback.payload[2];
    if (subPid !== 1 || granted !== 0) {
      throw new Error(`SUBACK 异常：packetId=${subPid} granted=${granted}`);
    }
    await writeLine("步骤2 完成：已订阅 bridge/#，授予 QoS 0");

    // ---- 步骤 3 + 4：MQTT 发布 → 帧协议 queue.lease 验证入队 ----
    // 提示：订阅 bridge/# 后，内核桥接泵会把匹配消息 fan-out 回订阅端，因此
    // 帧协议 queue.lease 与桥接泵存在竞争。这里先发出（挂起）帧租约请求，再执行
    // MQTT 发布，使消息一入队即被本进程抢到；若仍被泵取走，则重发并重试。
    const expectBody = "hello-from-mqtt";
    const leaseDeadline = Date.now() + READ_TIMEOUT + 3000;
    let leased = null;
    let published = false;
    while (Date.now() < leaseDeadline) {
      const remaining = Math.max(0, Math.min(3000, leaseDeadline - Date.now()));
      const leasePromise = ah.call({ m: "queue.lease", queue: "bridge/demo", waitMs: remaining });
      if (!published) {
        await mqtt.write(publishPacket("bridge/demo", expectBody, 0));
        published = true;
        await writeLine("步骤3 完成：MQTT 已发布 bridge/demo = hello-from-mqtt（QoS 0）");
      }
      const response = await leasePromise;
      const message = response && response.result ? response.result.message : null;
      if (message && message.body === expectBody) {
        leased = message;
        break;
      }
      // 未抢到（被桥接泵消费并 fan-out）：重新发布后再试一次
      await mqtt.write(publishPacket("bridge/demo", expectBody, 0));
    }
    if (!leased) {
      throw new Error("步骤4 失败：未能经帧协议从队列 bridge/demo 租约到 hello-from-mqtt");
    }
    await writeLine(`步骤4 完成：帧协议 queue.lease 取到内核队列消息 body=${leased.body}`);
    // 确认该消息，避免租约超时后重投造成回声
    try {
      await ah.call({ m: "queue.ack", msgId: leased.id });
    } catch {
      /* 确认失败不影响主流程 */
    }

    // ---- 步骤 5：帧协议发布 → MQTT 反向投递 ----
    const kernelBody = "hello-from-kernel";
    await ah.publish("bridge/demo", kernelBody);
    await writeLine("步骤5 进行中：帧协议已发布 bridge/demo = hello-from-kernel");
    const deadline = Date.now() + READ_TIMEOUT;
    let received = null;
    while (Date.now() < deadline) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) break;
      const pkt = await mqtt.read(remaining);
      if (pkt.type === TYPE.PUBLISH) {
        const parsed = parsePublish(pkt.flags, pkt.payload);
        if (parsed.topic === "bridge/demo" && parsed.payload === kernelBody) {
          received = parsed;
          break;
        }
        // 其余报文（例如步骤 3 被泵 fan-out 的回声）忽略
      }
    }
    if (!received) {
      throw new Error("步骤5 失败：未在 MQTT 连接上收到 PUBLISH topic=bridge/demo payload=hello-from-kernel");
    }
    await writeLine(`步骤5 完成：MQTT 收到 PUBLISH topic=${received.topic} payload=${received.payload}`);

    // ---- 步骤 6：PINGREQ → PINGRESP 保活验证 ----
    await mqtt.write(packet(TYPE.PINGREQ, 0, Buffer.alloc(0)));
    await readUntilType(mqtt, TYPE.PINGRESP, READ_TIMEOUT);
    await writeLine("步骤6 完成：PINGREQ → PINGRESP 保活正常");

    // ---- 步骤 7：DISCONNECT 并关闭连接 ----
    await mqtt.write(packet(TYPE.DISCONNECT, 0, Buffer.alloc(0)));
    await writeLine("步骤7 完成：已发送 DISCONNECT 并关闭连接");

    // ---- 步骤 8：成功 ----
    await writeLine("MQTT_BRIDGE_OK");
  } finally {
    if (mqtt) mqtt.close();
    if (ah) ah.close();
  }
}

main()
  .then(() => {
    process.exitCode = 0;
  })
  .catch((err) => {
    process.stderr.write(`MQTT_BRIDGE_ERROR: ${(err && err.stack) || err}\n`);
    process.exitCode = 1;
  });
