//! # AgentHeart 内核
//!
//! AI Agent 的"心脏"：心跳驱动的任务调度与消息处理内核。
//!
//! ## 设计约束
//!
//! - **零第三方依赖**：`Cargo.toml` 的 `[dependencies]` 为空，仅使用 Rust 标准库 `std`；
//! - **内核不含 UI**：UI 为内核外的可选产物（Tauri 2，位于 `agentheart-sdk/ui/`）；
//! - `unsafe` 仅允许出现在 `ffi` 模块（M5 引入）。
//!
//! 上述约束由 CI 门禁自动校验（见 `.github/workflows/ci.yml` 的零依赖断言）。
//!
//! ## 当前里程碑
//!
//! **M1 · 内核**：心跳驱动 + 任务框架（提交 / 异步执行 / 超时 / 重试）。
//!
//! ```
//! use agentheart_core::{Handler, Scheduler, SchedulerConfig, Task, TaskState};
//! use std::sync::Arc;
//!
//! let handler: Handler = Arc::new(|_task: &Task| Ok(()));
//! let scheduler = Scheduler::start_with(SchedulerConfig::default(), handler);
//! let id = scheduler.submit(Task::new("demo")).unwrap();
//! let task = scheduler.get(&id).unwrap();
//! assert!(matches!(
//!     task.state,
//!     TaskState::Pending | TaskState::Running | TaskState::Succeeded
//! ));
//! scheduler.shutdown();
//! ```

#![warn(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(unsafe_op_in_unsafe_fn)]
#![warn(clippy::all)]

pub mod automation;
pub mod clock;
pub mod concurrency;
pub mod error;
pub mod ffi;
pub mod json;
pub mod loops;
pub mod observe;
pub mod protocol;
pub mod queue;
pub mod scheduler;
pub mod storage;
pub mod task;
pub mod timer;

pub use automation::{Rule, RuleAction, RuleEngine, RuleFilter, RuleId, RuleSpec};
pub use clock::{Clock, ManualClock, SystemClock};
pub use concurrency::{FileLock, RateLimit};
pub use error::{Error, Result};
pub use loops::{Loop, LoopConfig, LoopController, LoopId, LoopOnError, LoopSpec, LoopState};
pub use observe::{Event, EventBus};
pub use protocol::{Frame, Kernel, MqttServer, Server};
pub use queue::{Broker, BrokerConfig, Message, MessageId, QueueStat};
pub use scheduler::{Handler, PauseScope, Scheduler, SchedulerConfig, SchedulerStats, TickHook};
pub use storage::Wal;
pub use task::{Priority, Task, TaskId, TaskSpan, TaskState};
pub use timer::{Cron, Job, JobId, JobState, MisfirePolicy, Schedule, utc_ms};
