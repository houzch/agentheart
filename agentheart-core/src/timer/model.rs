//! 定时任务模型：标识、调度方式、状态与错过补偿策略。

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::error::Result;
use crate::task::TaskId;

use super::cron::Cron;

/// 定时任务默认连续失败告警阈值。
const DEFAULT_MAX_CONSECUTIVE_FAILURES: u32 = 3;

/// 触发任务默认最大尝试次数（与 [`crate::task::Task`] 默认一致）。
const DEFAULT_TRIGGER_MAX_ATTEMPTS: u32 = 3;

/// 定时任务标识（不透明字符串）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JobId(String);

impl JobId {
    /// 由字符串构造。
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// 以 `&str` 视图访问。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// 定时任务状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    /// 启用。
    Enabled,
    /// 停用。
    Disabled,
}

impl JobState {
    /// 可读名称（`snake_case`）。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
        }
    }
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 错过补偿策略（系统休眠 / 延迟导致错过触发时）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MisfirePolicy {
    /// 跳过所有错过，直接对齐到未来。
    Skip,
    /// 只为最近一次错过补触发一次。
    FireOnce,
    /// 补齐错过（受 `SchedulerConfig::max_catch_up` 上限约束）。
    CatchUp,
}

impl MisfirePolicy {
    /// 可读名称（`snake_case`）。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Skip => "skip",
            Self::FireOnce => "fire_once",
            Self::CatchUp => "catch_up",
        }
    }
}

impl fmt::Display for MisfirePolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 调度方式。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Schedule {
    /// Cron 表达式。
    Cron(Cron),
    /// 固定间隔。
    Interval(Duration),
}

/// 定时任务。
#[derive(Debug, Clone)]
pub struct Job {
    /// 任务标识。
    pub id: JobId,
    /// 业务名（同时作为生成任务的名称）。
    pub name: String,
    /// 目标队列。
    pub queue: String,
    /// 调度方式。
    pub schedule: Schedule,
    /// 状态。
    pub state: JobState,
    /// 错过补偿策略。
    pub misfire: MisfirePolicy,
    /// 下次触发时间（Unix 毫秒）。
    pub next_run_at: Option<u64>,
    /// 上次触发时间（Unix 毫秒）。
    pub last_run_at: Option<u64>,
    /// 上次生成的任务 ID。
    pub last_task_id: Option<TaskId>,
    /// 上次已触发的调度槽位（用于周期去重）。
    pub last_slot: Option<u64>,
    /// 连续失败次数（由该定时任务触发的任务进入 `dead` 时累加，成功时清零）。
    pub consecutive_failures: u32,
    /// 连续失败告警阈值（达到该阈值的整数倍时投递 `event.error`）。
    pub max_consecutive_failures: u32,
    /// 每次触发所生成任务的最大尝试次数。
    pub max_attempts: u32,
}

impl Job {
    /// 由 Cron 表达式创建。
    ///
    /// # Errors
    /// Cron 表达式非法时返回错误。
    pub fn from_cron(
        name: impl Into<String>,
        queue: impl Into<String>,
        expression: &str,
    ) -> Result<Self> {
        Ok(Self {
            id: JobId(next_job_id()),
            name: name.into(),
            queue: queue.into(),
            schedule: Schedule::Cron(Cron::parse(expression)?),
            state: JobState::Enabled,
            misfire: MisfirePolicy::FireOnce,
            next_run_at: None,
            last_run_at: None,
            last_task_id: None,
            last_slot: None,
            consecutive_failures: 0,
            max_consecutive_failures: DEFAULT_MAX_CONSECUTIVE_FAILURES,
            max_attempts: DEFAULT_TRIGGER_MAX_ATTEMPTS,
        })
    }

    /// 由固定间隔创建。
    pub fn from_interval(
        name: impl Into<String>,
        queue: impl Into<String>,
        interval: Duration,
    ) -> Self {
        Self {
            id: JobId(next_job_id()),
            name: name.into(),
            queue: queue.into(),
            schedule: Schedule::Interval(interval),
            state: JobState::Enabled,
            misfire: MisfirePolicy::FireOnce,
            next_run_at: None,
            last_run_at: None,
            last_task_id: None,
            last_slot: None,
            consecutive_failures: 0,
            max_consecutive_failures: DEFAULT_MAX_CONSECUTIVE_FAILURES,
            max_attempts: DEFAULT_TRIGGER_MAX_ATTEMPTS,
        }
    }

    /// 设置连续失败告警阈值（至少为 1）。
    pub fn max_consecutive_failures(mut self, threshold: u32) -> Self {
        self.max_consecutive_failures = threshold.max(1);
        self
    }

    /// 设置每次触发所生成任务的最大尝试次数（至少为 1）。
    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts.max(1);
        self
    }

    /// 设置错过补偿策略。
    pub fn misfire_policy(mut self, policy: MisfirePolicy) -> Self {
        self.misfire = policy;
        self
    }

    /// 指定 ID。
    pub fn with_id(mut self, id: JobId) -> Self {
        self.id = id;
        self
    }

    /// 计算严格晚于 `after_ms` 的下一个调度槽位。
    pub fn next_slot_after(&self, after_ms: u64) -> Option<u64> {
        match &self.schedule {
            Schedule::Cron(cron) => cron.next_after(after_ms),
            Schedule::Interval(interval) => {
                let step = u64::try_from(interval.as_millis())
                    .unwrap_or(u64::MAX)
                    .max(1);
                Some(after_ms.saturating_add(step))
            }
        }
    }

    /// 以当前时间为基准刷新下一次触发时间。
    pub fn refresh_next(&mut self, now_ms: u64) {
        self.next_run_at = self.next_slot_after(now_ms);
    }
}

fn next_job_id() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(1);
    let sequence = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("j-{sequence}")
}
