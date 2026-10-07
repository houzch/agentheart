// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 循环任务模型：标识、状态机、规格与停止条件。
//!
//! 循环任务（Loop）以「迭代」为单位反复执行，直到满足停止条件
//! （迭代上限 / 截止时间 / 显式停止 / 失败策略）。时间统一为 **Unix 毫秒**。

use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::Error;
use crate::task::TaskId;

/// 循环任务默认目标队列（未指定时）。
pub(crate) const DEFAULT_QUEUE: &str = "loop";

/// 循环任务标识（不透明字符串）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LoopId(String);

impl LoopId {
    /// 由字符串构造。
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// 以 `&str` 视图访问。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LoopId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for LoopId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for LoopId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// 循环任务状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LoopState {
    /// 已创建，尚未开始首个迭代。
    Pending,
    /// 运行中（迭代推进中）。
    Running,
    /// 已暂停（保留进度，可恢复）。
    Paused,
    /// 已完成（达到迭代上限或正常收敛）。
    Completed,
    /// 已停止（显式停止 / 收敛信号 / 截止时间到）。
    Stopped,
    /// 已失败（按失败策略停止）。
    Failed,
    /// 已进入终态且不再自动推进（需人工干预）。
    Dead,
}

impl LoopState {
    /// 可读名称（`snake_case`，与内核接口协议一致）。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
            Self::Dead => "dead",
        }
    }

    /// 是否为终止态（不再自动推进）。
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Stopped | Self::Failed | Self::Dead
        )
    }

    /// 校验一次状态迁移是否合法。
    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Pending, Self::Running | Self::Paused | Self::Stopped)
                | (
                    Self::Running,
                    Self::Paused | Self::Completed | Self::Stopped | Self::Failed | Self::Dead
                )
                | (Self::Paused, Self::Running | Self::Stopped)
        )
    }
}

impl fmt::Display for LoopState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for LoopState {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "paused" => Ok(Self::Paused),
            "completed" => Ok(Self::Completed),
            "stopped" => Ok(Self::Stopped),
            "failed" => Ok(Self::Failed),
            "dead" => Ok(Self::Dead),
            _ => Err(Error::Protocol("unknown loop state")),
        }
    }
}

/// 迭代失败时的处理策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopOnError {
    /// 继续推进（失败计入迭代，直到达到迭代上限）。
    Continue,
    /// 立即停止（进入 `failed`）。
    Stop,
    /// 标记为 `dead`（终态，需人工干预）。
    Dead,
}

impl LoopOnError {
    /// 可读名称（`snake_case`）。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Continue => "continue",
            Self::Stop => "stop",
            Self::Dead => "dead",
        }
    }
}

impl fmt::Display for LoopOnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for LoopOnError {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "continue" => Ok(Self::Continue),
            "stop" => Ok(Self::Stop),
            "dead" => Ok(Self::Dead),
            _ => Err(Error::Protocol("unknown loop on-error policy")),
        }
    }
}

/// 循环任务规格（不可变参数）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoopSpec {
    /// 业务名（同时作为迭代任务的名称）。
    pub name: Option<String>,
    /// 迭代任务投递的目标队列。
    pub queue: String,
    /// 迭代次数上限（硬上限，防止跑飞）。
    pub max_iterations: u64,
    /// 迭代间隔（毫秒）。
    pub interval_ms: u64,
    /// 失败时是否按指数退避拉长间隔。
    pub backoff: bool,
    /// 单次迭代执行超时（毫秒，`None` 表示不限）。
    pub timeout_ms: Option<u64>,
    /// 绝对截止时间（Unix 毫秒，`None` 表示不限）。
    pub deadline_ms: Option<u64>,
    /// 迭代失败处理策略。
    pub on_error: LoopOnError,
    /// 幂等键（相同键重复创建只返回既有循环）。
    pub idempotency_key: Option<String>,
}

impl LoopSpec {
    /// 以目标队列创建规格（默认：单次迭代、无间隔、失败继续）。
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            name: None,
            queue: queue.into(),
            max_iterations: 1,
            interval_ms: 0,
            backoff: true,
            timeout_ms: None,
            deadline_ms: None,
            on_error: LoopOnError::Continue,
            idempotency_key: None,
        }
    }

    /// 设置业务名。
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// 设置迭代次数上限（至少为 1）。
    pub fn max_iterations(mut self, count: u64) -> Self {
        self.max_iterations = count.max(1);
        self
    }

    /// 设置迭代间隔（毫秒）。
    pub fn interval_ms(mut self, millis: u64) -> Self {
        self.interval_ms = millis;
        self
    }

    /// 设置失败指数退避。
    pub fn backoff(mut self, enabled: bool) -> Self {
        self.backoff = enabled;
        self
    }

    /// 设置单次迭代超时（毫秒）。
    pub fn timeout_ms(mut self, millis: u64) -> Self {
        self.timeout_ms = Some(millis);
        self
    }

    /// 设置绝对截止时间（Unix 毫秒）。
    pub fn deadline_ms(mut self, at: u64) -> Self {
        self.deadline_ms = Some(at);
        self
    }

    /// 设置失败处理策略。
    pub fn on_error(mut self, policy: LoopOnError) -> Self {
        self.on_error = policy;
        self
    }

    /// 设置幂等键。
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }
}

/// 循环任务记录。
#[derive(Debug, Clone)]
pub struct Loop {
    /// 循环 ID。
    pub id: LoopId,
    /// 规格。
    pub spec: LoopSpec,
    /// 当前状态。
    pub state: LoopState,
    /// 已完成（含失败）的迭代次数。
    pub iteration: u64,
    /// 连续失败次数。
    pub consecutive_failures: u32,
    /// 创建时间（Unix 毫秒）。
    pub created_at: u64,
    /// 最近更新时间（Unix 毫秒）。
    pub updated_at: u64,
    /// 首次开始时间（Unix 毫秒）。
    pub started_at: Option<u64>,
    /// 下次迭代的调度时间（Unix 毫秒；`None` 表示正在等待在途迭代）。
    pub next_run_at: Option<u64>,
    /// 最近一次迭代结果（`succeeded` / `failed`）。
    pub last_result: Option<String>,
    /// 最近一次失败信息。
    pub last_error: Option<String>,
    /// 在途迭代任务 ID（内部记账）。
    pub(crate) current_task: Option<TaskId>,
    /// 是否收到显式停止（收敛）信号。
    pub(crate) stop_requested: bool,
}

impl Loop {
    /// 创建新循环（状态为 `Pending`，首个迭代立即可跑）。
    pub(crate) fn new(id: LoopId, spec: LoopSpec, now: u64) -> Self {
        Self {
            id,
            spec,
            state: LoopState::Pending,
            iteration: 0,
            consecutive_failures: 0,
            created_at: now,
            updated_at: now,
            started_at: None,
            next_run_at: Some(now),
            last_result: None,
            last_error: None,
            current_task: None,
            stop_requested: false,
        }
    }

    /// 尝试迁移状态；非法迁移返回 [`Error::InvalidTransition`]（此处以 `Protocol` 表达）。
    pub(crate) fn set_state(&mut self, next: LoopState) -> Result<(), Error> {
        if self.state.can_transition_to(next) || self.state == next {
            self.state = next;
            Ok(())
        } else {
            Err(Error::Protocol("invalid loop state transition"))
        }
    }
}

/// 生成新的循环 ID。
pub(crate) fn next_loop_id() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(1);
    let sequence = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("l-{sequence}")
}
