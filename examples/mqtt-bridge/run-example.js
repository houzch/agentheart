"use strict";
/**
 * MQTT 桥接示例编排脚本。
 *
 * 启动侧车 `target/release/agentheartd.exe`，从 stdout 解析：
 * - `agentheartd ready addr=... token=...`
 * - `agentheartd mqtt=...`
 * 把三者作为环境变量注入并运行 `mqtt-bridge.js`，转发其输出并以相同退出码退出。
 */

const { spawn } = require("child_process");
const path = require("path");
const fs = require("fs");

const ROOT = path.join(__dirname, "..", "..");
const EXE = path.join(ROOT, "target", "release", "agentheartd.exe");
const BRIDGE = path.join(__dirname, "mqtt-bridge.js");

if (!fs.existsSync(EXE)) {
  console.error(`未找到侧车可执行文件：${EXE}`);
  console.error("请先构建：cargo build --release --bin agentheartd");
  process.exit(1);
}

console.log(`[runner] 启动侧车：${EXE}`);
const sidecar = spawn(EXE, [], { stdio: ["ignore", "pipe", "pipe"], windowsHide: true });

let addr = null;
let token = null;
let mqttAddr = null;
let outBuf = "";
let started = false;
let finished = false;

/** 结束：杀掉侧车并以给定退出码退出。 */
function shutdown(code) {
  if (finished) return;
  finished = true;
  clearTimeout(readyTimer);
  try {
    sidecar.kill();
  } catch {
    /* 忽略杀进程异常 */
  }
  process.exit(code);
}

// 等待侧车就绪的超时保护
const readyTimer = setTimeout(() => {
  console.error("[runner] 等待侧车就绪超时（未解析到 addr/token/mqtt）");
  shutdown(1);
}, 15000);

sidecar.stdout.on("data", (chunk) => {
  outBuf += chunk.toString("utf8");
  let index;
  while ((index = outBuf.indexOf("\n")) >= 0) {
    const line = outBuf.slice(0, index).replace(/\r$/, "");
    outBuf = outBuf.slice(index + 1);
    console.log(`[agentheartd] ${line}`);
    const ready = /agentheartd ready addr=(\S+) token=(\S+)/.exec(line);
    if (ready) {
      addr = ready[1];
      token = ready[2];
    }
    const mqtt = /agentheartd mqtt=(\S+)/.exec(line);
    if (mqtt) mqttAddr = mqtt[1];
    if (!started && addr && token && mqttAddr) startBridge();
  }
});

sidecar.stderr.on("data", (chunk) => process.stderr.write(`[agentheartd] ${chunk}`));

sidecar.on("exit", (code, signal) => {
  if (!finished) {
    console.error(`[runner] 侧车提前退出：code=${code} signal=${signal}`);
    shutdown(code === 0 ? 1 : (code === null ? 1 : code));
  }
});

/** 侧车就绪后运行桥接示例，并以其退出码结束编排。 */
function startBridge() {
  started = true;
  clearTimeout(readyTimer);
  console.log(`[runner] 侧车就绪 addr=${addr} mqtt=${mqttAddr}`);
  console.log("[runner] 运行 mqtt-bridge.js ...");
  const bridge = spawn(process.execPath, [BRIDGE], {
    env: { ...process.env, AH_MQTT_ADDR: mqttAddr, AH_ADDR: addr, AH_TOKEN: token },
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });
  bridge.stdout.on("data", (chunk) => process.stdout.write(chunk));
  bridge.stderr.on("data", (chunk) => process.stderr.write(chunk));
  // 用 close 而非 exit：确保子进程 stdio 全部关闭后再判定退出码，避免丢输出
  bridge.on("close", (code) => {
    console.log(`[runner] mqtt-bridge.js 退出码=${code}`);
    shutdown(code === null ? 1 : code);
  });
  bridge.on("error", (err) => {
    console.error(`[runner] 启动 mqtt-bridge.js 失败：${err.message}`);
    shutdown(1);
  });
}
