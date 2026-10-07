// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 消息模型：标识、消息体与队列统计。

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// 消息标识（不透明字符串）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId(String);

impl MessageId {
    /// 由字符串构造。
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// 以 `&str` 视图访问。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for MessageId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for MessageId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// 一条消息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// 消息 ID。
    pub id: MessageId,
    /// 所属队列。
    pub queue: String,
    /// 消息体。
    pub body: String,
    /// 已投递次数。
    pub attempts: u32,
    /// 创建时间（Unix 毫秒）。
    pub created_at: u64,
}

/// 队列统计。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueueStat {
    /// 队列名。
    pub name: String,
    /// 容量上限。
    pub capacity: usize,
    /// 可见（待消费）消息数。
    pub depth: usize,
    /// 在途（已投递未确认）消息数。
    pub inflight: usize,
    /// 累计投递数。
    pub delivered: u64,
    /// 累计确认数。
    pub acked: u64,
    /// 死信数。
    pub dead: usize,
    /// 近似消费速率（条/秒）。
    pub rate_per_sec: u64,
}

/// 生成消息 ID。
pub(crate) fn next_message_id() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(1);
    let sequence = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("m-{sequence}")
}
