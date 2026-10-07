# AgentHeart · AI Agent 的心脏包

**任务调度器 + 消息处理器**：心跳检测、每日定时、循环任务、并发冲突治理、消息缓冲与 MQTT 对接。
内核**零第三方依赖**（仅 Rust std），可编译为动态库 / 侧车 / 多语言 SDK，**轻量嵌入**各类 AI Agent 工具。

- 内核：`agentheart-core/`（Rust，edition 2024，`[dependencies]` 为空）
- SDK：`agentheart-sdk/`（Rust / Node / Python / Go / Java / C# + 适配器 + 可选 Tauri 2 控制台）
- 测试与基准：**独立私有仓库** `agentheart-test`（不随本仓库开源；依赖本仓库的 `agentheart-core/`）

## 快速开始（一条命令）

```powershell
powershell -ExecutionPolicy Bypass -File scripts/quickstart.ps1
```

脚本会：构建内核 + 侧车 → 启动侧车并解析 `addr` / `token` → 跑通随仓库分发的
SDK 冒烟测试（Python / Node）。

手动等价步骤：

```powershell
cargo build --release                        # 构建内核 + 侧车 + cdylib
cargo run -p agentheart-adapters --example daily_report        # 端到端示例：定时 → 循环 → 规则 → 队列
node examples/mqtt-bridge/run-example.js     # MQTT 3.1.1 桥接示例
```

> Go / Java SDK 冒烟与 FFI（ctypes / FFM）内嵌验证由私有测试仓库 `agentheart-test` 驱动，
> 以避免测试资产随本仓库开源。SDK 冒烟脚本本身（`agentheart-sdk/*/smoke_test.*`、`embed_test.py`）
> 仍在公开仓库中，可在侧车就绪后直接运行。

## 核心能力

| 能力 | 说明 |
| --- | --- |
| 任务调度 | 优先级队列 + 线程池、重试退避、死信、资源键串行、幂等去重、令牌桶限流、单次超时 |
| 定时任务 | 手写 Cron 解析、错过补偿（skip/fire_once/catch_up）、节律守护（连续失败告警） |
| 循环任务 | Loop 迭代推进、停止条件（次数/截止/信号）、失败策略与指数退避、WAL 回放 |
| 消息队列 | 有界队列 + 背压、至少一次投递 + 租约回收、死信、WAL 持久化 |
| 自动化规则 | 事件 → 动作（触发任务 / 发布消息 / 触发循环）、`maxFires`、幂等创建 |
| 心跳 | 自身心跳 + 自适应（空闲/繁忙）+ 业务心跳（卡死看门狗、过期租约兜底回收） |
| 协议 | 16 字节帧头 + JSON、游标分页、事件流（`event.task/delivery/error/loop/rule/heartbeat`）、C ABI |
| MQTT | MQTT 3.1.1 子集（QoS 0/1、通配订阅、遗嘱），topic 即队列名 |

## 接入方式

### 1. 侧车（推荐起步）

```powershell
target\release\agentheartd.exe      # 打印 addr / token / mqtt 端口
```

### 2. 各语言 SDK（Socket 承载）

```python
# agentheart-sdk/python/agentheart_client.py
from agentheart_client import Client
client = Client.connect("127.0.0.1:17890", token)
client.subscribe(["task"])
client.submit_task("report", "daily")
```

```javascript
// agentheart-sdk/node/agentheart.js
const { Client } = require("./agentheart-sdk/node/agentheart");
const client = await Client.connect("127.0.0.1:17890", token);
await client.subscribe(["task"]);
console.log(client.drainEvents());
```

Go / Java 见 `agentheart-sdk/go/`（`go run ./smoke`）与 `agentheart-sdk/java/`（`Smoke.java`）。

### 3. FFI 内嵌（进程内、无网络）

```python
# agentheart-sdk/python/embed_test.py —— ctypes 直接加载 cdylib
lib = ctypes.CDLL("target/release/agentheart_core.dll")
```

Java 用 Foreign Function & Memory API（`agentheart-sdk/java/Embed.java`，需 `--enable-native-access=ALL-UNNAMED`）。

### 4. 适配器（把 Agent 动作变成任务处理函数）

```rust
use agentheart_adapters::{CommandAdapter, HttpAdapter, McpAdapter, handler};

// 命令行
let adapter = CommandAdapter::from_shell("build", "cargo build --release");
// HTTP Webhook
let hook = HttpAdapter::new("hook", "http://127.0.0.1:9000/tasks")?;
// MCP 工具（stdio）
let mcp = McpAdapter::new("mcp", "npx", "agent.run").arg("-y").arg("some-mcp-server");

let handler = handler(std::sync::Arc::new(adapter));   // 注册进 Scheduler
```

### 5. 控制台 UI（可选）

```powershell
cargo run --manifest-path agentheart-sdk/ui/src-tauri/Cargo.toml
```

六大页面：总览 / 任务中心 / 定时任务 / 消息队列 / 链路追踪 / 循环任务；双承载（连接侧车或内嵌内核）。

## 质量门禁

```powershell
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all
cargo build --release
cargo tree -e normal -p agentheart-core     # 必须只输出 agentheart 自身（零第三方依赖）
```

基准回归基线与判定见私有测试仓库 `agentheart-test` 的 `benches/BASELINE.md`。

## 构建产物

```powershell
cargo build --release                                                        # 侧车 + 内核 cdylib
cargo build --release --manifest-path agentheart-sdk/ui/src-tauri/Cargo.toml # 可选控制台
```

产物：`target/release/agentheartd(.exe)`（侧车）、`agentheart_core.{dll,so,dylib}`（嵌入内核）、
`agentheart-ui(.exe)`（控制台）。

## 安装包

跨平台安装包由**内部发布流水线**产出，脚本位于私有仓库 `agentheart-test/scripts/`
（`release.ps1` / `bundle-ui.ps1` / `bundle-linux.ps1` + `bundle-linux.sh`），不随本仓库分发：

| 平台 | 产物 | 产出方式 |
| --- | --- | --- |
| Windows | `AgentHeart_<version>_x64-setup.exe` | Tauri NSIS（`bundle-ui.ps1`） |
| Linux | `.deb` / `.rpm` / `.AppImage` | 容器内构建（`bundle-linux.sh`，需 Docker） |
| macOS | `.dmg` / `.app` | 本仓库 [`release-macos.yml`](.github/workflows/release-macos.yml)（`macos-14` runner） |

macOS 包**无法在 Windows 上构建**（Tauri 打包依赖 macOS 宿主：clang/Xcode SDK、`hdiutil`、
`codesign`、`iconutil`），因此交由 macOS runner：在 Actions 页面手动触发 `Release (macOS)`
或推送 `v*` 标签，从 Artifacts 下载。

> **自行打包 UI**：在 `agentheart-sdk/ui` 下运行 `cargo tauri build`（需先
> `cargo install tauri-cli --locked`）。bundle targets 已按平台分流：基座 `tauri.conf.json`
> 不写死 targets，分别由 `tauri.windows.conf.json` / `tauri.linux.conf.json` /
> `tauri.macos.conf.json` 覆盖。
>
> **遇到 `os error 5（拒绝访问）`？** 刚执行完 `cargo install tauri-cli` 后的首次打包，
> `cargo tauri build` 可能在 `cargo metadata` 处报 `os error 5`：杀软/沙箱正在扫描新写入的
> `cargo-tauri.exe`，属**瞬时**占用（单独执行 `cargo metadata`、或稍后重跑同一命令均正常）。
> 等待几秒重试即可；必要时把 `cargo-tauri.exe` / `cargo.exe` 加入白名单。

CI：本仓库 `.github/workflows/ci.yml`（内核门禁 + 零依赖断言 + UI 独立工作区）；
跨语言 E2E 与集成测试在私有仓库 `agentheart-test` 的 CI 中执行。

## 许可证

本项目以 **MIT License** 开源，全文见 [LICENSE](LICENSE)（各 crate 的 `Cargo.toml` 均标注 `license = "MIT"`）。

Copyright (c) 2026 houzc

在软件的所有副本或实质性部分中保留上述版权声明与本许可声明的前提下，可自由使用、复制、修改、
合并、发布、分发、再许可和/或销售本软件。本软件按「原样」提供，不附带任何明示或暗示的担保。
