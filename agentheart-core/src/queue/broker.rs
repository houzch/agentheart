//! 消息代理：有界队列、背压、至少一次投递、死信与 WAL 持久化。
//!
//! - **有界 + 背压**：`publish` 在队列满时阻塞（`try_publish` 返回 [`Error::Busy`]）；
//! - **至少一次**：投递后进入在途，需 `ack`；租约到期未确认则重投；
//! - **死信**：重投次数达到上限后进入死信；
//! - **顺序**：单队列内 FIFO 保序；
//! - **持久化**：可选 WAL，记录 publish/ack/dead 并在启动时回放。

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crate::clock::Clock;
use crate::error::{Error, Result};
use crate::json::Value;
use crate::observe::EventBus;
use crate::storage::Wal;

use super::message::{Message, MessageId, QueueStat, next_message_id};

/// 消息代理配置。
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// 未显式声明容量时的默认队列容量。
    pub default_capacity: usize,
    /// 最大投递次数（超过则进入死信）。
    pub max_attempts: u32,
    /// 在途消息的可见性超时（租约时长）。
    pub visibility_timeout: Duration,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            default_capacity: 1024,
            max_attempts: 3,
            visibility_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Default)]
struct QueueState {
    ready: VecDeque<MessageId>,
    inflight: Vec<MessageId>,
    dead: Vec<MessageId>,
    messages: HashMap<MessageId, Message>,
    capacity: usize,
    delivered: u64,
    acked: u64,
    window_start_ms: u64,
    window_acked: u64,
    rate_per_sec: u64,
}

#[derive(Debug, Default)]
struct BrokerState {
    queues: HashMap<String, QueueState>,
    /// 在途租约：消息 ID -> (队列名, 到期 Unix 毫秒)。
    deadlines: HashMap<MessageId, (String, u64)>,
}

/// 一条被回收（租约到期）的在途消息。
#[derive(Debug)]
struct Reclaimed {
    queue: String,
    id: MessageId,
    dead: bool,
}

/// 消息代理。
pub struct Broker {
    state: Mutex<BrokerState>,
    not_empty: Condvar,
    not_full: Condvar,
    config: BrokerConfig,
    clock: Arc<dyn Clock>,
    wal: Option<Wal>,
    events: Mutex<Option<Arc<EventBus>>>,
    shutdown: AtomicBool,
}

impl fmt::Debug for Broker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let queues = lock(&self.state).queues.len();
        f.debug_struct("Broker")
            .field("config", &self.config)
            .field("queues", &queues)
            .field("journal", &self.wal.as_ref().map(Wal::path))
            .finish_non_exhaustive()
    }
}

impl Broker {
    /// 创建内存消息代理（无持久化）。
    pub fn new(config: BrokerConfig, clock: Arc<dyn Clock>) -> Self {
        Self {
            state: Mutex::new(BrokerState::default()),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
            config,
            clock,
            wal: None,
            events: Mutex::new(None),
            shutdown: AtomicBool::new(false),
        }
    }

    /// 创建带 WAL 的消息代理，并回放历史记录恢复状态。
    ///
    /// # Errors
    /// WAL 回放失败时返回错误。
    pub fn with_journal(config: BrokerConfig, clock: Arc<dyn Clock>, wal: Wal) -> Result<Self> {
        let broker = Self {
            state: Mutex::new(BrokerState::default()),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
            config,
            clock,
            wal: Some(wal),
            events: Mutex::new(None),
            shutdown: AtomicBool::new(false),
        };
        broker.recover()?;
        Ok(broker)
    }

    /// 绑定事件总线：此后会投递消息投递事件（`event.delivery`）。
    pub fn set_events(&self, events: Arc<EventBus>) {
        *lock(&self.events) = Some(events);
    }

    /// 绑定事件总线（builder 风格）。
    pub fn with_events(self, events: Arc<EventBus>) -> Self {
        self.set_events(events);
        self
    }

    /// 投递一条消息事件（未绑定总线时为空操作）。
    ///
    /// 锁序说明：该方法可能在内核状态锁内调用，仅额外获取总线自身的锁，
    /// 且**没有任何路径**先持总线锁再取内核状态锁，故无死锁风险。
    fn emit_delivery(&self, queue: &str, message_id: &MessageId, outcome: &str) {
        let bus = lock(&self.events).clone();
        let Some(bus) = bus else {
            return;
        };
        let data = vec![
            ("queue".to_string(), Value::String(queue.to_string())),
            ("msgId".to_string(), Value::String(message_id.to_string())),
            ("outcome".to_string(), Value::String(outcome.to_string())),
        ];
        bus.publish(self.clock.now_ms(), "delivery", data);
    }

    fn emit_reclaimed(&self, reclaimed: &[Reclaimed]) {
        for item in reclaimed {
            let outcome = if item.dead { "dead" } else { "requeued" };
            self.emit_delivery(&item.queue, &item.id, outcome);
        }
    }

    /// 投递一条错误事件（`event.error`；未绑定总线时为空操作）。
    fn emit_error(&self, error: &Error, reference: Option<&str>) {
        let bus = lock(&self.events).clone();
        let Some(bus) = bus else {
            return;
        };
        bus.publish_error(
            self.clock.now_ms(),
            error.code(),
            error.code_name(),
            &error.to_string(),
            reference,
        );
    }

    /// 声明队列及其容量。
    ///
    /// # Errors
    /// 写入 WAL 失败时返回错误。
    pub fn declare(&self, queue: impl Into<String>, capacity: usize) -> Result<()> {
        let queue = queue.into();
        let capacity = capacity.max(1);
        {
            let mut state = lock(&self.state);
            state
                .queues
                .entry(queue.clone())
                .or_insert_with(|| QueueState {
                    capacity,
                    ..QueueState::default()
                });
        }
        if let Err(error) = self.journal(
            "declare",
            &[
                ("q", Value::String(queue.clone())),
                ("cap", Value::Number(capacity as f64)),
            ],
        ) {
            self.emit_error(&error, Some(&queue));
            return Err(error);
        }
        Ok(())
    }

    /// 全部队列名。
    pub fn queues(&self) -> Vec<String> {
        let mut names: Vec<String> = lock(&self.state).queues.keys().cloned().collect();
        names.sort();
        names
    }

    /// 默认队列容量（新队列未显式声明时使用）。
    pub fn default_capacity(&self) -> usize {
        self.config.default_capacity.max(1)
    }

    /// 队列统计。
    pub fn stats(&self, queue: &str) -> Option<QueueStat> {
        let state = lock(&self.state);
        state.queues.get(queue).map(|q| QueueStat {
            name: queue.to_string(),
            capacity: q.capacity,
            depth: q.ready.len(),
            inflight: q.inflight.len(),
            delivered: q.delivered,
            acked: q.acked,
            dead: q.dead.len(),
            rate_per_sec: q.rate_per_sec,
        })
    }

    /// 发布消息（队列满时阻塞，直到有空位或关闭）。
    ///
    /// # Errors
    /// 代理已关闭或写入 WAL 失败时返回错误。
    pub fn publish(&self, queue: &str, body: impl Into<String>) -> Result<MessageId> {
        self.publish_inner(queue, body.into(), true)
    }

    /// 尝试发布消息（队列满时立即返回 [`Error::Busy`]）。
    ///
    /// # Errors
    /// 队列已满、代理已关闭或写入 WAL 失败时返回错误。
    pub fn try_publish(&self, queue: &str, body: impl Into<String>) -> Result<MessageId> {
        self.publish_inner(queue, body.into(), false)
    }

    /// 租约一条消息（至少一次投递）；`wait` 为最长等待时间。
    ///
    /// 返回的消息须在可见性超时前 [`Broker::ack`]，否则会被重投。
    pub fn lease(&self, queue: &str, wait: Duration) -> Option<Message> {
        let deadline = Instant::now() + wait;
        let mut guard = lock(&self.state);
        loop {
            let now = self.clock.now_ms();
            let reclaimed = reclaim(&mut guard, now, self.config.max_attempts);
            self.emit_reclaimed(&reclaimed);
            let visibility = millis(self.config.visibility_timeout);
            if let Some(message) = take_ready(&mut guard, queue, now, visibility) {
                self.emit_delivery(queue, &message.id, "delivered");
                drop(guard);
                self.not_full.notify_all();
                return Some(message);
            }
            if self.shutdown.load(Ordering::Acquire) {
                return None;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let (next, _) = self
                .not_empty
                .wait_timeout(guard, remaining.min(Duration::from_millis(50)))
                .unwrap_or_else(PoisonError::into_inner);
            guard = next;
        }
    }

    /// 确认消息（成功消费）。
    ///
    /// # Errors
    /// 消息不在途时返回 [`Error::NotFound`]。
    pub fn ack(&self, id: &MessageId) -> Result<()> {
        let queue = {
            let mut state = lock(&self.state);
            let Some((queue, _)) = state.deadlines.remove(id) else {
                return Err(Error::NotFound {
                    kind: "message",
                    id: id.to_string(),
                });
            };
            let now = self.clock.now_ms();
            if let Some(q) = state.queues.get_mut(&queue) {
                q.inflight.retain(|item| item != id);
                q.messages.remove(id);
                q.acked = q.acked.saturating_add(1);
                q.window_acked = q.window_acked.saturating_add(1);
                let elapsed = now.saturating_sub(q.window_start_ms);
                if elapsed >= 1000 {
                    q.rate_per_sec = q.window_acked.saturating_mul(1000) / elapsed.max(1);
                    q.window_start_ms = now;
                    q.window_acked = 0;
                }
            }
            queue
        };
        self.emit_delivery(&queue, id, "acked");
        self.journal(
            "ack",
            &[
                ("q", Value::String(queue)),
                ("id", Value::String(id.to_string())),
            ],
        )
    }

    /// 否定确认：`requeue=true` 重新入队；否则按重投次数决定重投或进入死信。
    ///
    /// 返回 `true` 表示该消息已进入死信队列。
    ///
    /// # Errors
    /// 消息不在途时返回 [`Error::NotFound`]。
    pub fn nack(&self, id: &MessageId, requeue: bool) -> Result<bool> {
        let (queue, to_dead) = {
            let mut state = lock(&self.state);
            let Some((queue, _)) = state.deadlines.remove(id) else {
                return Err(Error::NotFound {
                    kind: "message",
                    id: id.to_string(),
                });
            };
            let max_attempts = self.config.max_attempts;
            let mut to_dead = false;
            if let Some(q) = state.queues.get_mut(&queue) {
                q.inflight.retain(|item| item != id);
                let attempts = q.messages.get(id).map_or(0, |m| m.attempts);
                if requeue && attempts < max_attempts {
                    q.ready.push_front(id.clone());
                } else {
                    q.dead.push(id.clone());
                    to_dead = true;
                }
            }
            (queue, to_dead)
        };
        let op = if to_dead { "dead" } else { "requeue" };
        self.emit_delivery(&queue, id, if to_dead { "dead" } else { "requeued" });
        self.journal(
            op,
            &[
                ("q", Value::String(queue)),
                ("id", Value::String(id.to_string())),
            ],
        )?;
        self.not_empty.notify_all();
        Ok(to_dead)
    }

    /// 回收所有租约到期（超时未确认）的消息，返回回收条数。
    pub fn reclaim_expired(&self) -> usize {
        let now = self.clock.now_ms();
        let mut state = lock(&self.state);
        let reclaimed = reclaim(&mut state, now, self.config.max_attempts);
        drop(state);
        let count = reclaimed.len();
        if count > 0 {
            self.emit_reclaimed(&reclaimed);
            self.not_empty.notify_all();
        }
        count
    }

    /// 队列中的死信消息。
    pub fn dead_letters(&self, queue: &str) -> Vec<Message> {
        let state = lock(&self.state);
        let Some(q) = state.queues.get(queue) else {
            return Vec::new();
        };
        q.dead
            .iter()
            .filter_map(|id| q.messages.get(id).cloned())
            .collect()
    }

    /// 关闭代理：唤醒所有等待者。
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.not_empty.notify_all();
        self.not_full.notify_all();
    }

    fn publish_inner(&self, queue: &str, body: String, blocking: bool) -> Result<MessageId> {
        let default_capacity = self.config.default_capacity.max(1);
        let mut guard = lock(&self.state);
        if !guard.queues.contains_key(queue) {
            guard.queues.insert(
                queue.to_string(),
                QueueState {
                    capacity: default_capacity,
                    ..QueueState::default()
                },
            );
        }
        loop {
            let has_space = guard
                .queues
                .get(queue)
                .is_some_and(|q| q.ready.len() < q.capacity);
            if has_space {
                let id = MessageId::new(next_message_id());
                let created_at = self.clock.now_ms();
                if let Some(q) = guard.queues.get_mut(queue) {
                    q.messages.insert(
                        id.clone(),
                        Message {
                            id: id.clone(),
                            queue: queue.to_string(),
                            body: body.clone(),
                            attempts: 0,
                            created_at,
                        },
                    );
                    q.ready.push_back(id.clone());
                }
                drop(guard);
                if let Err(error) = self.journal(
                    "publish",
                    &[
                        ("q", Value::String(queue.to_string())),
                        ("id", Value::String(id.to_string())),
                        ("b", Value::String(body)),
                        ("t", Value::Number(created_at as f64)),
                    ],
                ) {
                    // 落盘失败：回滚内存状态，保持与 WAL 一致
                    self.rollback_publish(queue, &id);
                    self.emit_error(&error, Some(queue));
                    return Err(error);
                }
                self.emit_delivery(queue, &id, "published");
                self.not_empty.notify_one();
                return Ok(id);
            }
            if self.shutdown.load(Ordering::Acquire) {
                return Err(Error::Internal("broker is shutting down"));
            }
            if !blocking {
                return Err(Error::Busy);
            }
            let (next, _) = self
                .not_full
                .wait_timeout(guard, Duration::from_millis(100))
                .unwrap_or_else(PoisonError::into_inner);
            guard = next;
        }
    }

    /// 回滚一次落盘失败的消息（从内存队列中移除）。
    fn rollback_publish(&self, queue: &str, id: &MessageId) {
        let mut state = lock(&self.state);
        if let Some(q) = state.queues.get_mut(queue) {
            q.ready.retain(|item| item != id);
            q.messages.remove(id);
        }
    }

    fn journal(&self, op: &str, fields: &[(&str, Value)]) -> Result<()> {
        let Some(wal) = &self.wal else {
            return Ok(());
        };
        let mut record = Value::object();
        record.insert("op", Value::String(op.to_string()));
        for (key, value) in fields {
            record.insert(*key, value.clone());
        }
        wal.append(&record)
    }

    fn recover(&self) -> Result<()> {
        let Some(wal) = &self.wal else {
            return Ok(());
        };
        let records = wal.replay()?;
        let mut state = lock(&self.state);
        for record in records {
            let op = record.get("op").and_then(Value::as_str).unwrap_or("");
            let queue = record
                .get("q")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            match op {
                "declare" => {
                    let capacity =
                        record.get("cap").and_then(Value::as_f64).unwrap_or(0.0) as usize;
                    let capacity = capacity.max(1);
                    state.queues.entry(queue).or_insert_with(|| QueueState {
                        capacity,
                        ..QueueState::default()
                    });
                }
                "publish" => {
                    let id = record
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    let body = record
                        .get("b")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    let created_at = record.get("t").and_then(Value::as_f64).unwrap_or(0.0) as u64;
                    let entry = state
                        .queues
                        .entry(queue.clone())
                        .or_insert_with(|| QueueState {
                            capacity: self.config.default_capacity.max(1),
                            ..QueueState::default()
                        });
                    let id = MessageId::new(id);
                    entry.messages.insert(
                        id.clone(),
                        Message {
                            id: id.clone(),
                            queue,
                            body,
                            attempts: 0,
                            created_at,
                        },
                    );
                    entry.ready.push_back(id);
                }
                "ack" | "dead" => {
                    let id = MessageId::new(
                        record
                            .get("id")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string(),
                    );
                    if let Some(q) = state.queues.get_mut(&queue) {
                        if op == "dead" {
                            if q.messages.contains_key(&id) {
                                q.dead.push(id);
                            }
                        } else {
                            q.messages.remove(&id);
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

fn reclaim(state: &mut BrokerState, now_ms: u64, max_attempts: u32) -> Vec<Reclaimed> {
    let expired: Vec<MessageId> = state
        .deadlines
        .iter()
        .filter(|(_, (_, deadline))| *deadline <= now_ms)
        .map(|(id, _)| id.clone())
        .collect();
    let mut reclaimed = Vec::new();
    for id in expired {
        let Some((queue, _)) = state.deadlines.remove(&id) else {
            continue;
        };
        let mut dead = false;
        if let Some(q) = state.queues.get_mut(&queue) {
            q.inflight.retain(|item| item != &id);
            let attempts = q.messages.get(&id).map_or(0, |m| m.attempts);
            if attempts >= max_attempts {
                q.dead.push(id.clone());
                dead = true;
            } else {
                q.ready.push_front(id.clone());
            }
        }
        reclaimed.push(Reclaimed { queue, id, dead });
    }
    reclaimed
}

fn take_ready(
    state: &mut BrokerState,
    queue: &str,
    now_ms: u64,
    visibility_ms: u64,
) -> Option<Message> {
    let q = state.queues.get_mut(queue)?;
    let id = q.ready.pop_front()?;
    let message = q.messages.get_mut(&id)?;
    message.attempts = message.attempts.saturating_add(1);
    let delivered = message.clone();
    q.delivered = q.delivered.saturating_add(1);
    q.inflight.push(id.clone());
    let deadline = now_ms.saturating_add(visibility_ms);
    state.deadlines.insert(id, (queue.to_string(), deadline));
    Some(delivered)
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
