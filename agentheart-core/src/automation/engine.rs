//! 自动化规则引擎：订阅内核事件，命中规则即执行动作（事件驱动的日常自动化）。
//!
//! ## 工作方式
//!
//! 引擎随**心跳**（`Scheduler` 的 tick 钩子）轮询事件总线：从上次游标之后取出新事件，
//! 对每条事件匹配所有**启用**规则（事件类型 + 字段过滤器），命中即执行动作并投递
//! `event.rule`；单条事件的匹配与执行是同步、串行的。
//!
//! ## 动作
//!
//! - `trigger_job`：触发定时任务（`Scheduler::trigger_job`）；
//! - `publish`：向队列发布消息（`Broker::try_publish`）；
//! - `trigger_loop`：立即触发循环任务的一次迭代（`LoopController::trigger`，需启用循环引擎）。
//!
//! ## 安全阀
//!
//! `maxFires` 限定单条规则的最大触发次数（达到后自动停用）；规则启停即时生效；
//! 动作失败投递 `event.error`（不中断其余规则）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::error::{Error, Result};
use crate::json::Value;
use crate::loops::{LoopController, LoopId};
use crate::observe::{Event, EventBus, log};
use crate::queue::Broker;
use crate::scheduler::{Scheduler, TickHook};
use crate::storage::Wal;
use crate::timer::JobId;

use super::model::{Rule, RuleAction, RuleFilter, RuleId, RuleSpec, next_rule_id};

/// 支持监听的事件类型（`on` 取值；`"*"` 表示全部）。
pub const EVENT_KINDS: [&str; 5] = ["task", "delivery", "error", "heartbeat", "loop"];

#[derive(Debug, Default)]
struct Registry {
    rules: HashMap<RuleId, Rule>,
    order: Vec<RuleId>,
}

/// 自动化规则引擎。
#[derive(Debug)]
pub struct RuleEngine {
    scheduler: Arc<Scheduler>,
    broker: Arc<Broker>,
    loops: Mutex<Option<Arc<LoopController>>>,
    registry: Mutex<Registry>,
    /// 串行化 `tick` 与各变更操作。
    op: Mutex<()>,
    events: Mutex<Option<Arc<EventBus>>>,
    /// 已处理到的事件序号。
    cursor: AtomicU64,
    idempotency: Mutex<HashMap<String, RuleId>>,
    wal: Option<Wal>,
}

impl RuleEngine {
    /// 创建引擎（内存模式）。
    pub fn new(scheduler: Arc<Scheduler>, broker: Arc<Broker>) -> Self {
        Self::build(scheduler, broker, None)
    }

    /// 创建带 WAL 的引擎，并回放历史记录恢复规则。
    ///
    /// # Errors
    /// WAL 回放失败时返回错误。
    pub fn with_journal(scheduler: Arc<Scheduler>, broker: Arc<Broker>, wal: Wal) -> Result<Self> {
        let engine = Self::build(scheduler, broker, Some(wal));
        engine.recover()?;
        Ok(engine)
    }

    fn build(scheduler: Arc<Scheduler>, broker: Arc<Broker>, wal: Option<Wal>) -> Self {
        Self {
            scheduler,
            broker,
            loops: Mutex::new(None),
            registry: Mutex::new(Registry::default()),
            op: Mutex::new(()),
            events: Mutex::new(None),
            cursor: AtomicU64::new(0),
            idempotency: Mutex::new(HashMap::new()),
            wal,
        }
    }

    /// 绑定循环引擎：启用后 `trigger_loop` 动作才能生效。
    pub fn set_loops(&self, loops: Option<Arc<LoopController>>) {
        *lock(&self.loops) = loops;
    }

    /// 绑定事件总线：此后会订阅事件并投递 `event.rule`。
    pub fn set_events(&self, events: Arc<EventBus>) {
        *lock(&self.events) = Some(events);
    }

    /// 绑定事件总线（builder 风格）。
    pub fn with_events(self, events: Arc<EventBus>) -> Self {
        self.set_events(events);
        self
    }

    /// 生成心跳钩子：注册到 [`Scheduler::add_tick_hook`] 后即随心跳处理事件。
    pub fn hook(self: &Arc<Self>) -> TickHook {
        let engine = Arc::clone(self);
        Arc::new(move |now| engine.tick(now))
    }

    /// 已处理到的事件序号。
    pub fn cursor(&self) -> u64 {
        self.cursor.load(Ordering::Acquire)
    }

    /// 创建规则（`idempotency_key` 相同时返回既有规则 ID）。
    ///
    /// # Errors
    /// 事件类型不在白名单内时返回 [`Error::Protocol`]。
    pub fn create(&self, spec: RuleSpec) -> Result<RuleId> {
        if !is_known_kind(&spec.on) {
            return Err(Error::Protocol("unknown rule event kind"));
        }
        let _op = lock(&self.op);
        if let Some(key) = &spec.idempotency_key {
            if let Some(existing) = lock(&self.idempotency).get(key) {
                return Ok(existing.clone());
            }
        }
        let now = self.scheduler.now();
        let id = RuleId::new(next_rule_id());
        let idempotency_key = spec.idempotency_key.clone();
        let rule = Rule::new(id.clone(), spec, now);
        lock(&self.registry).order.push(id.clone());
        if let Some(key) = idempotency_key {
            lock(&self.idempotency).insert(key, id.clone());
        }
        self.persist(&rule);
        self.emit(&rule, None, "created");
        Ok(id)
    }

    /// 查询规则快照。
    pub fn get(&self, id: &RuleId) -> Option<Rule> {
        lock(&self.registry).rules.get(id).cloned()
    }

    /// 列出全部规则（按创建顺序）。
    pub fn list(&self) -> Vec<Rule> {
        let registry = lock(&self.registry);
        registry
            .order
            .iter()
            .filter_map(|id| registry.rules.get(id).cloned())
            .collect()
    }

    /// 启用 / 停用规则。
    ///
    /// # Errors
    /// 规则不存在时返回 [`Error::NotFound`]。
    pub fn set_enabled(&self, id: &RuleId, enabled: bool) -> Result<bool> {
        let _op = lock(&self.op);
        let mut rule = self.take(id)?;
        if rule.enabled != enabled {
            rule.enabled = enabled;
            rule.updated_at = self.scheduler.now();
            self.persist(&rule);
            self.emit(&rule, None, if enabled { "enabled" } else { "disabled" });
        }
        Ok(rule.enabled)
    }

    /// 删除规则（幂等：不存在时返回 `false`）。
    pub fn delete(&self, id: &RuleId) -> bool {
        let _op = lock(&self.op);
        let removed = lock(&self.registry).rules.remove(id).is_some();
        if removed {
            lock(&self.registry).order.retain(|item| item != id);
        }
        removed
    }

    /// 处理一次（随心跳调用）：消费自游标之后的新事件，命中规则即执行动作。
    pub fn tick(&self, now: u64) {
        let bus = lock(&self.events).clone();
        let Some(bus) = bus else {
            return;
        };
        let from = self.cursor.load(Ordering::Acquire);
        let events = bus.poll_after(from);
        if events.is_empty() {
            return;
        }
        let mut newest = from;
        for event in &events {
            newest = event.seq;
            self.dispatch(event, now);
        }
        self.cursor.store(newest, Ordering::Release);
    }

    // ---- 内部实现 ----

    fn take(&self, id: &RuleId) -> Result<Rule> {
        lock(&self.registry)
            .rules
            .get(id)
            .cloned()
            .ok_or_else(|| Error::NotFound {
                kind: "rule",
                id: id.to_string(),
            })
    }

    /// 分发一条事件：找出命中且启用的规则并依次执行。
    fn dispatch(&self, event: &Event, now: u64) {
        let candidates: Vec<Rule> = lock(&self.registry)
            .rules
            .values()
            .filter(|rule| {
                rule.enabled
                    && (rule.on == "*" || rule.on == event.kind)
                    && rule.filter.matches(event)
            })
            .cloned()
            .collect();
        for rule in candidates {
            if !self.reserve(&rule, now) {
                continue;
            }
            self.run_action(&rule, event);
            self.emit(&rule, Some(event.seq), "fired");
        }
    }

    /// 记账一次触发（含 `maxFires` 上限与自动停用）；未执行返回 `false`。
    fn reserve(&self, rule: &Rule, now: u64) -> bool {
        let mut registry = lock(&self.registry);
        let Some(entry) = registry.rules.get_mut(&rule.id) else {
            return false;
        };
        if !entry.enabled {
            return false;
        }
        entry.fires = entry.fires.saturating_add(1);
        entry.updated_at = now;
        let reached = entry.max_fires.is_some_and(|max| entry.fires >= max);
        if reached {
            entry.enabled = false;
            log::info(&format!("规则 {} 已达最大触发次数，自动停用", entry.id));
        }
        let snapshot = entry.clone();
        drop(registry);
        self.persist(&snapshot);
        true
    }

    fn run_action(&self, rule: &Rule, event: &Event) {
        let outcome = match &rule.action {
            RuleAction::TriggerJob { job_id } => {
                self.scheduler.trigger_job(&JobId::new(job_id)).map(|_| ())
            }
            RuleAction::Publish { queue, body } => self.broker.try_publish(queue, body).map(|_| ()),
            RuleAction::TriggerLoop { loop_id } => match lock(&self.loops).clone() {
                Some(loops) => loops.trigger(&LoopId::new(loop_id)).map(|_| ()),
                None => {
                    log::warn(&format!(
                        "规则 {} 的 trigger_loop 动作被忽略：循环引擎未启用",
                        rule.id
                    ));
                    Ok(())
                }
            },
        };
        if let Err(error) = outcome {
            log::warn(&format!("规则 {} 动作执行失败: {error}", rule.id));
            self.emit_error(&error, rule.id.as_str(), Some(event.seq));
        }
    }

    fn emit(&self, rule: &Rule, event_seq: Option<u64>, phase: &str) {
        let bus = lock(&self.events).clone();
        let Some(bus) = bus else {
            return;
        };
        let mut data = vec![
            ("ruleId".to_string(), Value::String(rule.id.to_string())),
            ("on".to_string(), Value::String(rule.on.clone())),
            (
                "action".to_string(),
                Value::String(rule.action.kind().to_string()),
            ),
            ("phase".to_string(), Value::String(phase.to_string())),
            ("fires".to_string(), Value::Number(rule.fires as f64)),
        ];
        if let Some(seq) = event_seq {
            data.push(("eventSeq".to_string(), Value::Number(seq as f64)));
        }
        bus.publish(rule.updated_at, "rule", data);
    }

    fn emit_error(&self, error: &Error, reference: &str, event_seq: Option<u64>) {
        let bus = lock(&self.events).clone();
        let Some(bus) = bus else {
            return;
        };
        let message = match event_seq {
            Some(seq) => format!("{error} (event #{seq})"),
            None => error.to_string(),
        };
        bus.publish_error(
            self.scheduler.now(),
            error.code(),
            error.code_name(),
            &message,
            Some(reference),
        );
    }

    fn persist(&self, rule: &Rule) {
        lock(&self.registry)
            .rules
            .insert(rule.id.clone(), rule.clone());
        if let Err(error) = self.journal(rule) {
            log::warn(&format!("规则 WAL 写入失败: {error}"));
            self.emit_error(&error, rule.id.as_str(), None);
        }
    }

    fn journal(&self, rule: &Rule) -> Result<()> {
        let Some(wal) = &self.wal else {
            return Ok(());
        };
        let mut record = Value::object();
        record.insert("op", Value::String("rule".to_string()));
        record.insert("id", Value::String(rule.id.to_string()));
        if let Some(name) = &rule.name {
            record.insert("n", Value::String(name.clone()));
        }
        record.insert("on", Value::String(rule.on.clone()));
        record.insert("filter", rule.filter.to_value());
        record.insert("action", rule.action.to_value());
        record.insert("en", Value::Bool(rule.enabled));
        record.insert("fires", Value::Number(rule.fires as f64));
        if let Some(max) = rule.max_fires {
            record.insert("max", Value::Number(max as f64));
        }
        record.insert("ts", Value::Number(rule.created_at as f64));
        record.insert("up", Value::Number(rule.updated_at as f64));
        wal.append(&record)
    }

    fn recover(&self) -> Result<()> {
        let Some(wal) = &self.wal else {
            return Ok(());
        };
        let records = wal.replay()?;
        let mut registry = lock(&self.registry);
        for record in records {
            if record.get("op").and_then(Value::as_str) != Some("rule") {
                continue;
            }
            let Some(id) = record.get("id").and_then(Value::as_str) else {
                continue;
            };
            let Some(on) = record.get("on").and_then(Value::as_str) else {
                continue;
            };
            let action = record.get("action").map_or_else(
                || Err(Error::Protocol("rule record missing action")),
                RuleAction::from_value,
            );
            let Ok(action) = action else {
                continue;
            };
            let filter = record
                .get("filter")
                .and_then(|value| RuleFilter::from_value(value).ok())
                .unwrap_or_default();
            let created_at = number(&record, "ts");
            let spec = RuleSpec {
                name: record.get("n").and_then(Value::as_str).map(str::to_string),
                on: on.to_string(),
                filter,
                action,
                enabled: record.get("en").and_then(Value::as_bool).unwrap_or(true),
                max_fires: record
                    .get("max")
                    .and_then(Value::as_f64)
                    .map(|value| value as u64),
                idempotency_key: None,
            };
            let mut rule = Rule::new(RuleId::new(id), spec, created_at);
            rule.fires = number(&record, "fires");
            rule.updated_at = number(&record, "up").max(created_at);
            let rule_id = rule.id.clone();
            if !registry.order.contains(&rule_id) {
                registry.order.push(rule_id.clone());
            }
            registry.rules.insert(rule_id, rule);
        }
        Ok(())
    }
}

fn is_known_kind(kind: &str) -> bool {
    kind == "*" || EVENT_KINDS.contains(&kind)
}

fn number(record: &Value, key: &str) -> u64 {
    record.get(key).and_then(Value::as_f64).unwrap_or(0.0) as u64
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
