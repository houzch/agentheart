// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 任务模型：标识、状态机、优先级与任务记录。
//!
//! 时间戳统一为 **Unix 毫秒**（与内核接口协议一致，见方案 8.6）。

use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::error::{Error, Result};

/// 任务默认最大尝试次数。
const DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// 每个任务保留的链路跨度上限。
const TASK_SPANS_LIMIT: usize = 16;

/// 任务执行跨度（用于链路追踪）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSpan {
    /// 跨度名称（`run` / `retry`）。
    pub name: String,
    /// 开始时间（Unix 毫秒）。
    pub start_ts: u64,
    /// 持续时长（毫秒）。
    pub dur_ms: u64,
    /// 本次执行是否成功。
    pub ok: bool,
    /// 附加信息（失败时为错误描述）。
    pub detail: Option<String>,
}

/// 任务标识（不透明字符串）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TaskId(String);

impl TaskId {
    /// 由字符串构造。
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// 以 `&str` 视图访问。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for TaskId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for TaskId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// 任务优先级：数值**越大越优先**。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Priority(i32);

impl Priority {
    /// 低优先级。
    pub const LOW: Self = Self(-1);
    /// 默认优先级。
    pub const NORMAL: Self = Self(0);
    /// 高优先级。
    pub const HIGH: Self = Self(1);

    /// 构造。
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    /// 取值。
    pub const fn get(self) -> i32 {
        self.0
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 任务状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskState {
    /// 待执行。
    Pending,
    /// 执行中。
    Running,
    /// 已成功。
    Succeeded,
    /// 已失败（可能重试）。
    Failed,
    /// 等待重试。
    Retrying,
    /// 失败且不再重试。
    Dead,
    /// 已取消。
    Canceled,
    /// 已暂停。
    Paused,
}

impl TaskState {
    /// 可读名称（`snake_case`，与内核接口协议一致）。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Retrying => "retrying",
            Self::Dead => "dead",
            Self::Canceled => "canceled",
            Self::Paused => "paused",
        }
    }

    /// 是否为终止态（不再变化）。
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Dead | Self::Canceled)
    }

    /// 校验一次状态迁移是否合法。
    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Pending, Self::Running | Self::Paused | Self::Canceled)
                | (Self::Paused, Self::Pending | Self::Canceled)
                | (
                    Self::Running,
                    Self::Succeeded | Self::Failed | Self::Canceled
                )
                | (Self::Failed, Self::Retrying | Self::Dead | Self::Canceled)
                | (Self::Retrying, Self::Pending | Self::Canceled)
                | (Self::Dead, Self::Retrying | Self::Canceled)
        )
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TaskState {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self> {
        match text {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "retrying" => Ok(Self::Retrying),
            "dead" => Ok(Self::Dead),
            "canceled" => Ok(Self::Canceled),
            "paused" => Ok(Self::Paused),
            _ => Err(Error::Protocol("unknown task state")),
        }
    }
}

/// 任务记录。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    /// 任务 ID。
    pub id: TaskId,
    /// 业务名（可选）。
    pub name: Option<String>,
    /// 所属队列。
    pub queue: String,
    /// 当前状态。
    pub state: TaskState,
    /// 优先级。
    pub priority: Priority,
    /// 已尝试次数。
    pub attempts: u32,
    /// 最大尝试次数。
    pub max_attempts: u32,
    /// 创建时间（Unix 毫秒），由内核在**提交时**写入。
    pub created_at: Option<u64>,
    /// 开始执行时间（Unix 毫秒，最近一次尝试）。
    pub started_at: Option<u64>,
    /// 首次开始执行时间（Unix 毫秒）。
    pub first_started_at: Option<u64>,
    /// 执行链路跨度（有界，保留最近 [`TASK_SPANS_LIMIT`] 条）。
    pub spans: Vec<TaskSpan>,
    /// 结束时间（Unix 毫秒）。
    pub finished_at: Option<u64>,
    /// 下次重试时间（Unix 毫秒）。
    pub next_retry_at: Option<u64>,
    /// 标签。
    pub tags: Vec<String>,
    /// 最近一次错误信息。
    pub last_error: Option<String>,
    /// 载荷引用（避免大对象直传）。
    pub payload_ref: Option<String>,
    /// 资源键：同一键的任务串行执行（会话亲和 / 资源互斥）。
    pub key: Option<String>,
    /// 幂等键：同一键重复提交只入队一次。
    pub idempotency_key: Option<String>,
    /// 单次执行超时（`None` 表示不限）。
    pub timeout: Option<Duration>,
}

impl Task {
    /// 创建待执行任务（自动生成 ID）。
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            id: TaskId(next_task_id()),
            name: None,
            queue: queue.into(),
            state: TaskState::Pending,
            priority: Priority::NORMAL,
            attempts: 0,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            created_at: None,
            started_at: None,
            first_started_at: None,
            spans: Vec::new(),
            finished_at: None,
            next_retry_at: None,
            tags: Vec::new(),
            last_error: None,
            payload_ref: None,
            key: None,
            idempotency_key: None,
            timeout: None,
        }
    }

    /// 指定 ID 创建（用于幂等提交或状态回放）。
    pub fn with_id(id: TaskId, queue: impl Into<String>) -> Self {
        let mut task = Self::new(queue);
        task.id = id;
        task
    }

    /// 设置业务名。
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// 设置优先级。
    pub fn priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// 设置最大尝试次数（至少为 1）。
    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts.max(1);
        self
    }

    /// 设置单次执行超时。
    pub fn timeout(mut self, limit: Duration) -> Self {
        self.timeout = Some(limit);
        self
    }

    /// 追加标签。
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// 设置载荷引用。
    pub fn payload_ref(mut self, reference: impl Into<String>) -> Self {
        self.payload_ref = Some(reference.into());
        self
    }

    /// 设置资源键（同键任务串行执行）。
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// 设置幂等键（同键重复提交只入队一次）。
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// 状态迁移；非法迁移返回 [`Error::InvalidTransition`]。
    ///
    /// # Errors
    /// 当 `next` 不是当前状态的合法后继时返回错误。
    pub fn transition(&mut self, next: TaskState) -> Result<()> {
        if self.state.can_transition_to(next) {
            self.state = next;
            Ok(())
        } else {
            Err(Error::InvalidTransition {
                from: self.state,
                to: next,
            })
        }
    }

    /// 取消任务（幂等：终止态下为空操作）。
    ///
    /// # Errors
    /// 迁移非法时返回 [`Error::InvalidTransition`]（正常路径不会发生）。
    pub fn cancel(&mut self) -> Result<()> {
        if self.state.is_terminal() {
            return Ok(());
        }
        self.transition(TaskState::Canceled)
    }

    /// 追加一条执行跨度（超出上限时丢弃最早的）。
    pub fn push_span(&mut self, span: TaskSpan) {
        self.spans.push(span);
        if self.spans.len() > TASK_SPANS_LIMIT {
            let excess = self.spans.len() - TASK_SPANS_LIMIT;
            self.spans.drain(0..excess);
        }
    }
}

fn next_task_id() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(1);
    let sequence = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("t-{sequence}")
}
