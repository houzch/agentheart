// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 循环任务控制器：随心跳推进迭代、把迭代包装为任务、串行执行与失败策略。
//!
//! ## 工作方式
//!
//! 控制器随**心跳**（`Scheduler` 的 tick 钩子）推进：每个到期循环提交**一个迭代任务**
//! （复用 M1 任务框架），待其在途任务进入终态后再决定「继续下一迭代 / 完成 / 停止 / 失败」。
//! 同一循环的迭代**严格串行**（任意时刻至多一个在途迭代），并以资源键 `loop:<id>` 兜底。
//!
//! ## 停止条件
//!
//! 迭代上限（`max_iterations`）、绝对截止（`deadline_ms`）、显式停止（`stop` /
//! 收敛信号 `signal_stop`）、失败策略（[`LoopOnError`]）。

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::concurrency::random::Rng;
use crate::error::{Error, Result};
use crate::json::Value;
use crate::observe::{EventBus, log};
use crate::scheduler::{Scheduler, TickHook};
use crate::storage::Wal;
use crate::task::{Task, TaskId, TaskState};

use super::model::{DEFAULT_QUEUE, Loop, LoopId, LoopOnError, LoopSpec, LoopState, next_loop_id};

/// 循环控制器配置。
#[derive(Debug, Clone, Copy)]
pub struct LoopConfig {
    /// 失败退避的上限（毫秒）。
    pub max_backoff_ms: u64,
    /// 失败退避是否加入抖动（默认关闭以保证确定性）。
    pub jitter: bool,
}

impl Default for LoopConfig {
    fn default() -> Self {
        Self {
            max_backoff_ms: 300_000,
            jitter: false,
        }
    }
}

#[derive(Debug, Default)]
struct Registry {
    loops: HashMap<LoopId, Loop>,
    order: Vec<LoopId>,
}

/// 循环任务控制器。
#[derive(Debug)]
pub struct LoopController {
    scheduler: Arc<Scheduler>,
    registry: Mutex<Registry>,
    /// 串行化 `tick` 与各变更操作（避免在途迭代被重复提交）。
    op: Mutex<()>,
    events: Mutex<Option<Arc<EventBus>>>,
    config: LoopConfig,
    rng: Mutex<Rng>,
    idempotency: Mutex<HashMap<String, LoopId>>,
    wal: Option<Wal>,
}

impl LoopController {
    /// 创建控制器（内存模式，默认配置）。
    pub fn new(scheduler: Arc<Scheduler>) -> Self {
        Self::build(scheduler, LoopConfig::default(), None)
    }

    /// 以指定配置创建控制器（内存模式）。
    pub fn with_config(scheduler: Arc<Scheduler>, config: LoopConfig) -> Self {
        Self::build(scheduler, config, None)
    }

    /// 创建带 WAL 的控制器，并回放历史记录恢复循环状态。
    ///
    /// # Errors
    /// WAL 回放失败时返回错误。
    pub fn with_journal(scheduler: Arc<Scheduler>, config: LoopConfig, wal: Wal) -> Result<Self> {
        let controller = Self::build(scheduler, config, Some(wal));
        controller.recover()?;
        Ok(controller)
    }

    fn build(scheduler: Arc<Scheduler>, config: LoopConfig, wal: Option<Wal>) -> Self {
        Self {
            scheduler,
            registry: Mutex::new(Registry::default()),
            op: Mutex::new(()),
            events: Mutex::new(None),
            config,
            rng: Mutex::new(Rng::from_entropy()),
            idempotency: Mutex::new(HashMap::new()),
            wal,
        }
    }

    /// 绑定事件总线：此后会投递 `event.loop` 事件。
    pub fn set_events(&self, events: Arc<EventBus>) {
        *lock(&self.events) = Some(events);
    }

    /// 绑定事件总线（builder 风格）。
    pub fn with_events(self, events: Arc<EventBus>) -> Self {
        self.set_events(events);
        self
    }

    /// 生成心跳钩子：注册到 [`Scheduler::add_tick_hook`] 后即随心跳推进。
    pub fn hook(self: &Arc<Self>) -> TickHook {
        let controller = Arc::clone(self);
        Arc::new(move |now| controller.tick(now))
    }

    /// 创建循环任务（`idempotency_key` 相同时返回既有循环 ID）。
    ///
    /// # Errors
    /// `max_iterations` 为 0 时返回 [`Error::Protocol`]。
    pub fn create(&self, mut spec: LoopSpec) -> Result<LoopId> {
        if spec.queue.is_empty() {
            spec.queue = DEFAULT_QUEUE.to_string();
        }
        if spec.max_iterations == 0 {
            return Err(Error::Protocol("loop maxIterations must be >= 1"));
        }
        let _op = lock(&self.op);
        if let Some(key) = &spec.idempotency_key {
            if let Some(existing) = lock(&self.idempotency).get(key) {
                return Ok(existing.clone());
            }
        }
        let now = self.scheduler.now();
        let id = LoopId::new(next_loop_id());
        let item = Loop::new(id.clone(), spec, now);
        lock(&self.registry).order.push(id.clone());
        if let Some(key) = item.spec.idempotency_key.clone() {
            lock(&self.idempotency).insert(key, id.clone());
        }
        self.persist(&item);
        self.emit(&item, "created");
        Ok(id)
    }

    /// 查询循环任务快照。
    pub fn get(&self, id: &LoopId) -> Option<Loop> {
        lock(&self.registry).loops.get(id).cloned()
    }

    /// 列出全部循环任务（按创建顺序）。
    pub fn list(&self) -> Vec<Loop> {
        let registry = lock(&self.registry);
        registry
            .order
            .iter()
            .filter_map(|id| registry.loops.get(id).cloned())
            .collect()
    }

    /// 暂停循环任务（幂等）。
    ///
    /// # Errors
    /// 循环不存在时返回 [`Error::NotFound`]。
    pub fn pause(&self, id: &LoopId) -> Result<LoopState> {
        let _op = lock(&self.op);
        let mut item = self.take(id)?;
        if item.state.is_terminal() || item.state == LoopState::Paused {
            return Ok(item.state);
        }
        item.set_state(LoopState::Paused)?;
        item.updated_at = self.scheduler.now();
        self.persist(&item);
        self.emit(&item, "paused");
        Ok(item.state)
    }

    /// 恢复循环任务（幂等）；恢复后首个迭代立即到期。
    ///
    /// # Errors
    /// 循环不存在时返回 [`Error::NotFound`]。
    pub fn resume(&self, id: &LoopId) -> Result<LoopState> {
        let _op = lock(&self.op);
        let mut item = self.take(id)?;
        if item.state != LoopState::Paused {
            return Ok(item.state);
        }
        item.set_state(LoopState::Running)?;
        item.updated_at = self.scheduler.now();
        item.next_run_at = Some(item.updated_at);
        self.persist(&item);
        self.emit(&item, "resumed");
        Ok(item.state)
    }

    /// 停止循环任务（幂等）：进入 `stopped`。
    ///
    /// # Errors
    /// 循环不存在时返回 [`Error::NotFound`]。
    pub fn stop(&self, id: &LoopId) -> Result<LoopState> {
        let _op = lock(&self.op);
        let mut item = self.take(id)?;
        if item.state.is_terminal() {
            return Ok(item.state);
        }
        item.set_state(LoopState::Stopped)?;
        item.next_run_at = None;
        item.updated_at = self.scheduler.now();
        self.persist(&item);
        self.emit(&item, "stopped");
        Ok(item.state)
    }

    /// 立即触发一次迭代；若已有在途迭代则返回该任务 ID（幂等）。
    ///
    /// # Errors
    /// 循环不存在（[`Error::NotFound`]）、已终止或已达迭代上限（[`Error::Protocol`]）、
    /// 或已暂停（[`Error::Conflict`]）时返回错误。
    pub fn trigger(&self, id: &LoopId) -> Result<TaskId> {
        let _op = lock(&self.op);
        let mut item = self.take(id)?;
        if item.state.is_terminal() {
            return Err(Error::Protocol("loop is already terminal"));
        }
        if let Some(task_id) = item.current_task.clone() {
            return Ok(task_id);
        }
        if item.state == LoopState::Paused {
            return Err(Error::Conflict {
                resource: id.to_string(),
            });
        }
        if item.iteration >= item.spec.max_iterations {
            return Err(Error::Protocol("loop reached max iterations"));
        }
        let now = self.scheduler.now();
        if item.state == LoopState::Pending {
            item.set_state(LoopState::Running)?;
            item.started_at = Some(now);
        }
        let task_id = self.submit_iteration(&item)?;
        item.current_task = Some(task_id.clone());
        item.next_run_at = None;
        item.updated_at = now;
        self.persist(&item);
        self.emit(&item, "started");
        Ok(task_id)
    }

    /// 由迭代任务发出「收敛/停止」信号：任务资源键形如 `loop:<id>`。
    ///
    /// 宿主可在处理函数内调用本方法，令循环在本次迭代后优雅停止。
    pub fn signal_stop(&self, task: &Task) -> bool {
        let Some(key) = task.key.as_deref() else {
            return false;
        };
        let Some(rest) = key.strip_prefix("loop:") else {
            return false;
        };
        let _op = lock(&self.op);
        match lock(&self.registry).loops.get_mut(&LoopId::new(rest)) {
            Some(item) => {
                item.stop_requested = true;
                true
            }
            None => false,
        }
    }

    /// 推进一次（随心跳调用）：对每个循环观测在途迭代或启动下一个迭代。
    pub fn tick(&self, now: u64) {
        let _op = lock(&self.op);
        let ids: Vec<LoopId> = lock(&self.registry).order.clone();
        for id in ids {
            self.advance(&id, now);
        }
    }

    // ---- 内部实现 ----

    fn take(&self, id: &LoopId) -> Result<Loop> {
        lock(&self.registry)
            .loops
            .get(id)
            .cloned()
            .ok_or_else(|| Error::NotFound {
                kind: "loop",
                id: id.to_string(),
            })
    }

    fn advance(&self, id: &LoopId, now: u64) {
        let Some(mut item) = self.get(id) else {
            return;
        };
        if item.state.is_terminal() {
            return;
        }
        if let Some(task_id) = item.current_task.clone() {
            self.observe_iteration(&mut item, &task_id, now);
            return;
        }
        self.maybe_next(&mut item, now);
    }

    fn observe_iteration(&self, item: &mut Loop, task_id: &TaskId, now: u64) {
        let Some(task) = self.scheduler.get(task_id) else {
            self.finish_iteration(
                item,
                false,
                Some("iteration task vanished".to_string()),
                now,
            );
            return;
        };
        match task.state {
            TaskState::Succeeded => self.finish_iteration(item, true, None, now),
            TaskState::Dead | TaskState::Canceled => {
                self.finish_iteration(item, false, task.last_error.clone(), now);
            }
            _ => {}
        }
    }

    fn maybe_next(&self, item: &mut Loop, now: u64) {
        if item.stop_requested {
            self.finish_terminal(item, LoopState::Stopped, "stopped", now);
            return;
        }
        if item.iteration >= item.spec.max_iterations {
            self.finish_terminal(item, LoopState::Completed, "completed", now);
            return;
        }
        if let Some(deadline) = item.spec.deadline_ms {
            if now >= deadline {
                self.finish_terminal(item, LoopState::Stopped, "stopped", now);
                return;
            }
        }
        if item.state == LoopState::Paused {
            return;
        }
        if item.state == LoopState::Pending {
            if let Err(error) = item.set_state(LoopState::Running) {
                log::warn(&format!("循环 {} 无法进入运行态: {error}", item.id));
                return;
            }
            item.started_at = Some(now);
        }
        if let Some(due) = item.next_run_at {
            if due > now {
                return;
            }
        }
        self.start_iteration(item, now);
    }

    fn start_iteration(&self, item: &mut Loop, now: u64) {
        match self.submit_iteration(item) {
            Ok(task_id) => {
                item.current_task = Some(task_id);
                item.next_run_at = None;
                item.updated_at = now;
                self.persist(item);
                self.emit(item, "started");
            }
            Err(error) => {
                log::warn(&format!("循环 {} 提交迭代任务失败: {error}", item.id));
                self.emit_error(&error, item.id.as_str());
            }
        }
    }

    fn finish_iteration(&self, item: &mut Loop, ok: bool, error: Option<String>, now: u64) {
        item.iteration = item.iteration.saturating_add(1);
        item.current_task = None;
        item.updated_at = now;
        if ok {
            item.consecutive_failures = 0;
            item.last_result = Some("succeeded".to_string());
            item.last_error = None;
            if item.iteration >= item.spec.max_iterations {
                self.finish_terminal(item, LoopState::Completed, "completed", now);
            } else {
                item.next_run_at = Some(now.saturating_add(item.spec.interval_ms));
                self.persist(item);
                self.emit(item, "iteration");
            }
        } else {
            item.consecutive_failures = item.consecutive_failures.saturating_add(1);
            item.last_result = Some("failed".to_string());
            item.last_error = error;
            self.apply_failure(item, now);
        }
    }

    fn apply_failure(&self, item: &mut Loop, now: u64) {
        if item.stop_requested {
            self.finish_terminal(item, LoopState::Stopped, "stopped", now);
            return;
        }
        match item.spec.on_error {
            LoopOnError::Continue => {
                if item.iteration >= item.spec.max_iterations {
                    self.finish_terminal(item, LoopState::Failed, "failed", now);
                } else {
                    let delay = self.backoff_ms(item);
                    item.next_run_at = Some(now.saturating_add(delay));
                    self.persist(item);
                    self.emit(item, "iteration");
                }
            }
            LoopOnError::Stop => self.finish_terminal(item, LoopState::Failed, "failed", now),
            LoopOnError::Dead => self.finish_terminal(item, LoopState::Dead, "dead", now),
        }
    }

    fn finish_terminal(&self, item: &mut Loop, next: LoopState, phase: &str, now: u64) {
        if let Err(error) = item.set_state(next) {
            log::warn(&format!("循环 {} 状态迁移失败: {error}", item.id));
            return;
        }
        item.next_run_at = None;
        item.updated_at = now;
        self.persist(item);
        self.emit(item, phase);
    }

    fn backoff_ms(&self, item: &Loop) -> u64 {
        let base = item.spec.interval_ms.min(self.config.max_backoff_ms);
        if !item.spec.backoff || item.consecutive_failures <= 1 {
            return base;
        }
        let shift = item.consecutive_failures.saturating_sub(1).min(20);
        let delay = base
            .saturating_mul(1_u64 << shift)
            .min(self.config.max_backoff_ms);
        if self.config.jitter {
            let span = (delay / 4).max(1);
            delay.saturating_add(lock(&self.rng).next_below(span))
        } else {
            delay
        }
    }

    fn submit_iteration(&self, item: &Loop) -> Result<TaskId> {
        let key = format!("loop:{}", item.id);
        let mut task = Task::new(item.spec.queue.clone())
            .max_attempts(1)
            .key(key.clone())
            .tag(key);
        if let Some(name) = &item.spec.name {
            task = task.name(name.clone());
        }
        if let Some(ms) = item.spec.timeout_ms {
            task = task.timeout(Duration::from_millis(ms));
        }
        self.scheduler.submit(task)
    }

    fn emit(&self, item: &Loop, phase: &str) {
        let bus = lock(&self.events).clone();
        let Some(bus) = bus else {
            return;
        };
        let mut data = vec![
            ("loopId".to_string(), Value::String(item.id.to_string())),
            ("phase".to_string(), Value::String(phase.to_string())),
            (
                "state".to_string(),
                Value::String(item.state.as_str().to_string()),
            ),
            (
                "iteration".to_string(),
                Value::Number(item.iteration as f64),
            ),
        ];
        if let Some(result) = &item.last_result {
            data.push(("result".to_string(), Value::String(result.clone())));
        }
        bus.publish(item.updated_at, "loop", data);
    }

    fn emit_error(&self, error: &Error, reference: &str) {
        let bus = lock(&self.events).clone();
        let Some(bus) = bus else {
            return;
        };
        bus.publish_error(
            self.scheduler.now(),
            error.code(),
            error.code_name(),
            &error.to_string(),
            Some(reference),
        );
    }

    fn persist(&self, item: &Loop) {
        lock(&self.registry)
            .loops
            .insert(item.id.clone(), item.clone());
        if let Err(error) = self.journal(item) {
            log::warn(&format!("循环 WAL 写入失败: {error}"));
            self.emit_error(&error, item.id.as_str());
        }
    }

    fn journal(&self, item: &Loop) -> Result<()> {
        let Some(wal) = &self.wal else {
            return Ok(());
        };
        let mut record = Value::object();
        record.insert("op", Value::String("loop".to_string()));
        record.insert("id", Value::String(item.id.to_string()));
        record.insert("q", Value::String(item.spec.queue.clone()));
        record.insert("max", Value::Number(item.spec.max_iterations as f64));
        record.insert("itv", Value::Number(item.spec.interval_ms as f64));
        record.insert("bo", Value::Bool(item.spec.backoff));
        if let Some(name) = &item.spec.name {
            record.insert("n", Value::String(name.clone()));
        }
        if let Some(ms) = item.spec.timeout_ms {
            record.insert("to", Value::Number(ms as f64));
        }
        if let Some(at) = item.spec.deadline_ms {
            record.insert("dl", Value::Number(at as f64));
        }
        record.insert("oe", Value::String(item.spec.on_error.as_str().to_string()));
        if let Some(key) = &item.spec.idempotency_key {
            record.insert("ik", Value::String(key.clone()));
        }
        record.insert("st", Value::String(item.state.as_str().to_string()));
        record.insert("it", Value::Number(item.iteration as f64));
        record.insert("ts", Value::Number(item.created_at as f64));
        record.insert("up", Value::Number(item.updated_at as f64));
        if let Some(next) = item.next_run_at {
            record.insert("nx", Value::Number(next as f64));
        }
        wal.append(&record)
    }

    fn recover(&self) -> Result<()> {
        let Some(wal) = &self.wal else {
            return Ok(());
        };
        let records = wal.replay()?;
        let mut registry = lock(&self.registry);
        for record in records {
            if record.get("op").and_then(Value::as_str) != Some("loop") {
                continue;
            }
            let Some(id) = record.get("id").and_then(Value::as_str) else {
                continue;
            };
            let spec = LoopSpec {
                name: record.get("n").and_then(Value::as_str).map(str::to_string),
                queue: record
                    .get("q")
                    .and_then(Value::as_str)
                    .unwrap_or(DEFAULT_QUEUE)
                    .to_string(),
                max_iterations: number(&record, "max").max(1),
                interval_ms: number(&record, "itv"),
                backoff: record.get("bo").and_then(Value::as_bool).unwrap_or(true),
                timeout_ms: optional_number(&record, "to"),
                deadline_ms: optional_number(&record, "dl"),
                on_error: record
                    .get("oe")
                    .and_then(Value::as_str)
                    .and_then(|text| text.parse().ok())
                    .unwrap_or(LoopOnError::Continue),
                idempotency_key: record.get("ik").and_then(Value::as_str).map(str::to_string),
            };
            let state = record
                .get("st")
                .and_then(Value::as_str)
                .and_then(|text| text.parse().ok())
                .unwrap_or(LoopState::Pending);
            let created_at = number(&record, "ts");
            let mut item = Loop::new(LoopId::new(id), spec, created_at);
            item.state = state;
            item.iteration = number(&record, "it");
            item.updated_at = number(&record, "up").max(created_at);
            item.next_run_at = optional_number(&record, "nx");
            item.started_at = (state != LoopState::Pending).then_some(created_at);
            let loop_id = item.id.clone();
            if !registry.order.contains(&loop_id) {
                registry.order.push(loop_id.clone());
            }
            registry.loops.insert(loop_id, item);
        }
        Ok(())
    }
}

fn number(record: &Value, key: &str) -> u64 {
    record.get(key).and_then(Value::as_f64).unwrap_or(0.0) as u64
}

fn optional_number(record: &Value, key: &str) -> Option<u64> {
    record
        .get(key)
        .and_then(Value::as_f64)
        .map(|value| value as u64)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
