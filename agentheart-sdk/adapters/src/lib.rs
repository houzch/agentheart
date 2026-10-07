//! AgentHeart 接入适配器：把 Agent 动作映射为内核任务处理函数（M11）。
//!
//! 定位：本 crate **位于内核之外**（内核依赖树不含它），把「一次任务执行」映射为
//! 具体 Agent 动作，再用 [`handler`] / [`router`] 转成内核可注册的 [`Handler`]。
//!
//! 内置适配器：
//!
//! | 适配器 | 动作 | 典型场景 |
//! | --- | --- | --- |
//! | [`CommandAdapter`] | 执行命令行 | 把 shell / 构建 / 脚本作为任务 |
//! | [`HttpAdapter`] | 发起 HTTP 请求 | Webhook 回调、外部服务编排 |
//! | [`McpAdapter`] | 调用 MCP 工具 | 对接 claude / cursor / trae 等 MCP 生态 |
//!
//! 仅使用 `std` 与内核 crate（无第三方依赖）。

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use agentheart_core::observe::log;
use agentheart_core::{Error, Handler, Task};

mod command;
mod http;
mod mcp;

pub use command::CommandAdapter;
pub use http::HttpAdapter;
pub use mcp::McpAdapter;

/// 适配器错误（可携带动态消息）。
#[derive(Debug)]
pub struct AdapterError(String);

impl AdapterError {
    /// 由消息构造。
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    /// 错误消息。
    pub fn message(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for AdapterError {}

impl From<std::io::Error> for AdapterError {
    fn from(error: std::io::Error) -> Self {
        Self(error.to_string())
    }
}

/// 适配器结果。
pub type AdapterResult<T> = Result<T, AdapterError>;

/// Agent 动作适配器：把一次任务执行映射为具体动作。
pub trait AgentAdapter: Send + Sync + 'static {
    /// 适配器名称（日志与路由用）。
    fn name(&self) -> &str;

    /// 执行一次任务；`Ok(())` 表示动作成功。
    ///
    /// 采用 `&self`，故适配器内部需自行保证并发安全；内核可并发调用。
    fn call(&self, task: &Task) -> AdapterResult<()>;
}

/// 动态适配器句柄。
pub type DynAdapter = Arc<dyn AgentAdapter>;

/// 把单个适配器转成内核 [`Handler`]。
///
/// 适配器失败时记录日志，并向内核返回 `INTERNAL` 错误（由内核重试 / 死信策略接管）。
pub fn handler(adapter: DynAdapter) -> Handler {
    Arc::new(move |task: &Task| dispatch(&adapter, task))
}

/// 按**任务队列名**路由到不同适配器的 [`Handler`]。
///
/// 未命中路由时使用 `fallback`；两者都没有则返回 `BAD_REQUEST`。
pub fn router(routes: Vec<(String, DynAdapter)>, fallback: Option<DynAdapter>) -> Handler {
    let table: HashMap<String, DynAdapter> = routes.into_iter().collect();
    Arc::new(move |task: &Task| match table.get(&task.queue) {
        Some(adapter) => dispatch(adapter, task),
        None => match &fallback {
            Some(adapter) => dispatch(adapter, task),
            None => {
                log::warn(&format!("队列 {} 未配置适配器", task.queue));
                Err(Error::Protocol("no adapter for task queue"))
            }
        },
    })
}

fn dispatch(adapter: &DynAdapter, task: &Task) -> Result<(), Error> {
    match adapter.call(task) {
        Ok(()) => Ok(()),
        Err(error) => {
            log::warn(&format!(
                "适配器 {} 执行任务 {} 失败: {error}",
                adapter.name(),
                task.id
            ));
            Err(Error::Internal("adapter action failed"))
        }
    }
}
