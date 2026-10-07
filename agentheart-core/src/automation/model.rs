// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 自动化规则模型：规则标识、事件过滤、动作与规则记录。
//!
//! 规则描述「某类事件命中某些字段时，执行某个动作」，是 §6.5「事件驱动的日常自动化」的核心。

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Error, Result};
use crate::json::Value;
use crate::observe::Event;

/// 规则标识（不透明字符串）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RuleId(String);

impl RuleId {
    /// 由字符串构造。
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// 以 `&str` 视图访问。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RuleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// 事件字段过滤器：多个 `字段 == 值` 条件取**逻辑与**；空过滤器表示匹配该事件类型的全部事件。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuleFilter {
    /// `(字段名, 期望字符串值)` 列表。
    pub matches: Vec<(String, String)>,
}

impl RuleFilter {
    /// 从 `{ "字段": "值", ... }` 对象解析。
    ///
    /// # Errors
    /// 非对象或值非字符串时返回 [`Error::Protocol`]。
    pub fn from_value(value: &Value) -> Result<Self> {
        let Some(entries) = value.as_object() else {
            return Err(Error::Protocol("rule filter must be an object"));
        };
        let mut matches = Vec::with_capacity(entries.len());
        for (key, item) in entries {
            let Some(text) = item.as_str() else {
                return Err(Error::Protocol("rule filter values must be strings"));
            };
            matches.push((key.clone(), text.to_string()));
        }
        Ok(Self { matches })
    }

    /// 转换为 JSON 对象。
    pub fn to_value(&self) -> Value {
        let mut object = Value::object();
        for (key, value) in &self.matches {
            object.insert(key.clone(), Value::String(value.clone()));
        }
        object
    }

    /// 事件是否命中本过滤器。
    pub fn matches(&self, event: &Event) -> bool {
        self.matches.iter().all(|(key, expected)| {
            event
                .data
                .iter()
                .any(|(name, value)| name == key && value.as_str() == Some(expected.as_str()))
        })
    }
}

/// 规则命中后执行的动作。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleAction {
    /// 触发一个定时任务（`Scheduler::trigger_job`）。
    TriggerJob {
        /// 定时任务 ID。
        job_id: String,
    },
    /// 向队列发布一条消息（`Broker::try_publish`）。
    Publish {
        /// 目标队列。
        queue: String,
        /// 消息体。
        body: String,
    },
    /// 立即触发一个循环任务的一次迭代（`LoopController::trigger`）。
    TriggerLoop {
        /// 循环任务 ID。
        loop_id: String,
    },
}

impl RuleAction {
    /// 从动作对象解析，形如 `{ "type": "...", ... }`。
    ///
    /// # Errors
    /// 缺少 `type` 或必填字段、或类型未知时返回 [`Error::Protocol`]。
    pub fn from_value(value: &Value) -> Result<Self> {
        let Some(kind) = value.get("type").and_then(Value::as_str) else {
            return Err(Error::Protocol("rule action requires type"));
        };
        match kind {
            "trigger_job" => {
                let Some(job_id) = value.get("jobId").and_then(Value::as_str) else {
                    return Err(Error::Protocol("trigger_job requires jobId"));
                };
                Ok(Self::TriggerJob {
                    job_id: job_id.to_string(),
                })
            }
            "publish" => {
                let (Some(queue), Some(body)) = (
                    value.get("queue").and_then(Value::as_str),
                    value.get("body").and_then(Value::as_str),
                ) else {
                    return Err(Error::Protocol("publish requires queue and body"));
                };
                Ok(Self::Publish {
                    queue: queue.to_string(),
                    body: body.to_string(),
                })
            }
            "trigger_loop" => {
                let Some(loop_id) = value.get("loopId").and_then(Value::as_str) else {
                    return Err(Error::Protocol("trigger_loop requires loopId"));
                };
                Ok(Self::TriggerLoop {
                    loop_id: loop_id.to_string(),
                })
            }
            _ => Err(Error::Protocol("unknown rule action type")),
        }
    }

    /// 动作类型名。
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::TriggerJob { .. } => "trigger_job",
            Self::Publish { .. } => "publish",
            Self::TriggerLoop { .. } => "trigger_loop",
        }
    }

    /// 转换为 JSON 对象。
    pub fn to_value(&self) -> Value {
        let mut value = Value::object();
        value.insert("type", Value::String(self.kind().to_string()));
        match self {
            Self::TriggerJob { job_id } => {
                value.insert("jobId", Value::String(job_id.clone()));
            }
            Self::Publish { queue, body } => {
                value.insert("queue", Value::String(queue.clone()));
                value.insert("body", Value::String(body.clone()));
            }
            Self::TriggerLoop { loop_id } => {
                value.insert("loopId", Value::String(loop_id.clone()));
            }
        }
        value
    }
}

/// 规则的创建规格（由宿主或协议层构造后交给 [`crate::automation::RuleEngine::create`]）。
#[derive(Debug, Clone)]
pub struct RuleSpec {
    /// 业务名（可选）。
    pub name: Option<String>,
    /// 监听的事件类型。
    pub on: String,
    /// 事件字段过滤器。
    pub filter: RuleFilter,
    /// 命中后执行的动作。
    pub action: RuleAction,
    /// 是否启用（默认 `true`）。
    pub enabled: bool,
    /// 最大触发次数（`None` 表示不限）。
    pub max_fires: Option<u64>,
    /// 幂等键（相同键重复创建只返回既有规则）。
    pub idempotency_key: Option<String>,
}

impl RuleSpec {
    /// 以事件类型与动作创建规格（默认启用、不限次数、空过滤器）。
    pub fn new(on: impl Into<String>, action: RuleAction) -> Self {
        Self {
            name: None,
            on: on.into(),
            filter: RuleFilter::default(),
            action,
            enabled: true,
            max_fires: None,
            idempotency_key: None,
        }
    }

    /// 设置业务名。
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// 设置事件字段过滤器。
    pub fn filter(mut self, filter: RuleFilter) -> Self {
        self.filter = filter;
        self
    }

    /// 设置是否启用。
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// 设置最大触发次数（至少为 1）。
    pub fn max_fires(mut self, count: u64) -> Self {
        self.max_fires = Some(count.max(1));
        self
    }

    /// 设置幂等键。
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }
}

/// 一条自动化规则。
#[derive(Debug, Clone)]
pub struct Rule {
    /// 规则 ID。
    pub id: RuleId,
    /// 业务名（可选）。
    pub name: Option<String>,
    /// 监听的事件类型（`task` / `delivery` / `error` / `heartbeat` / `loop`；`"*"` 表示全部）。
    pub on: String,
    /// 事件字段过滤器。
    pub filter: RuleFilter,
    /// 命中后执行的动作。
    pub action: RuleAction,
    /// 是否启用。
    pub enabled: bool,
    /// 已触发次数。
    pub fires: u64,
    /// 最大触发次数（`None` 表示不限）；达到后自动停用。
    pub max_fires: Option<u64>,
    /// 创建时间（Unix 毫秒）。
    pub created_at: u64,
    /// 最近更新时间（Unix 毫秒）。
    pub updated_at: u64,
}

impl Rule {
    /// 依据规格创建规则（`fires` 从 0 开始）。
    pub(crate) fn new(id: RuleId, spec: RuleSpec, now: u64) -> Self {
        Self {
            id,
            name: spec.name,
            on: spec.on,
            filter: spec.filter,
            action: spec.action,
            enabled: spec.enabled,
            fires: 0,
            max_fires: spec.max_fires,
            created_at: now,
            updated_at: now,
        }
    }
}

/// 生成新的规则 ID。
pub(crate) fn next_rule_id() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(1);
    let sequence = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("r-{sequence}")
}
