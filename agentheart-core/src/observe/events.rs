//! 事件流：任务生命周期等事件的单调递增序号与重放窗口。
//!
//! 位于 `observe` 层，供内核（调度器）投递、由内核接口对外推送。

use std::collections::VecDeque;
use std::sync::{Condvar, Mutex, PoisonError};
use std::time::Duration;

use crate::json::Value;

/// 一条事件。
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    /// 单调递增序号。
    pub seq: u64,
    /// 时间戳（Unix 毫秒）。
    pub ts: u64,
    /// 事件类型（如 `task`）。
    pub kind: String,
    /// 载荷字段。
    pub data: Vec<(String, Value)>,
}

impl Event {
    /// 转换为 JSON 值（`{ m: "event.<kind>", seq, ts, ... }`）。
    pub fn to_value(&self) -> Value {
        let mut value = Value::object();
        value.insert("m", Value::String(format!("event.{}", self.kind)));
        value.insert("seq", Value::Number(self.seq as f64));
        value.insert("ts", Value::Number(self.ts as f64));
        for (key, item) in &self.data {
            value.insert(key.clone(), item.clone());
        }
        value
    }
}

#[derive(Debug)]
struct EventState {
    events: VecDeque<Event>,
    next_seq: u64,
    capacity: usize,
}

/// 事件总线：发布、查询与阻塞等待。
#[derive(Debug)]
pub struct EventBus {
    state: Mutex<EventState>,
    cv: Condvar,
}

impl EventBus {
    /// 以重放窗口容量创建。
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(EventState {
                events: VecDeque::new(),
                next_seq: 1,
                capacity: capacity.max(1),
            }),
            cv: Condvar::new(),
        }
    }

    /// 发布一条事件，返回其序号。
    pub fn publish(&self, ts: u64, kind: &str, data: Vec<(String, Value)>) -> u64 {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let seq = state.next_seq;
        state.next_seq = state.next_seq.saturating_add(1);
        state.events.push_back(Event {
            seq,
            ts,
            kind: kind.to_string(),
            data,
        });
        while state.events.len() > state.capacity {
            state.events.pop_front();
        }
        drop(state);
        self.cv.notify_all();
        seq
    }

    /// 发布一条错误事件（`event.error`）。
    ///
    /// 载荷：`{ code, codeName, message, ref? }`，`ref` 为出错对象引用（任务 ID / 队列名 / 锁路径等）。
    pub fn publish_error(
        &self,
        ts: u64,
        code: i32,
        code_name: &str,
        message: &str,
        reference: Option<&str>,
    ) -> u64 {
        let mut data = vec![
            ("code".to_string(), Value::Number(f64::from(code))),
            ("codeName".to_string(), Value::String(code_name.to_string())),
            ("message".to_string(), Value::String(message.to_string())),
        ];
        if let Some(reference) = reference {
            data.push(("ref".to_string(), Value::String(reference.to_string())));
        }
        self.publish(ts, "error", data)
    }

    /// 返回序号在 `(after_seq, +∞)` 内的事件（非阻塞）。
    pub fn poll_after(&self, after_seq: u64) -> Vec<Event> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .events
            .iter()
            .filter(|event| event.seq > after_seq)
            .cloned()
            .collect()
    }

    /// 最新序号。
    pub fn latest_seq(&self) -> u64 {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.next_seq.saturating_sub(1)
    }

    /// 重放窗口内最旧事件的序号（用于检测缺口）。
    pub fn oldest_seq(&self) -> u64 {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .events
            .front()
            .map_or(state.next_seq, |event| event.seq)
    }

    /// 阻塞等待 `(after_seq, +∞)` 内的事件，最多等待 `timeout`。
    pub fn wait_after(&self, after_seq: u64, timeout: Duration) -> Vec<Event> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !has_newer(&state, after_seq) && timeout > Duration::ZERO {
            let (next, _) = self
                .cv
                .wait_timeout(state, timeout)
                .unwrap_or_else(PoisonError::into_inner);
            state = next;
        }
        state
            .events
            .iter()
            .filter(|event| event.seq > after_seq)
            .cloned()
            .collect()
    }
}

fn has_newer(state: &EventState, after_seq: u64) -> bool {
    state
        .events
        .back()
        .is_some_and(|event| event.seq > after_seq)
}
