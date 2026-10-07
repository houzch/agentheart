//! 内核上下文与消息分发（协议第 8.6 节的实现）。

use std::sync::Arc;
use std::time::Duration;

use crate::automation::{Rule, RuleAction, RuleEngine, RuleFilter, RuleId, RuleSpec};
use crate::error::Error;
use crate::json::Value;
use crate::loops::{Loop, LoopController, LoopId, LoopOnError, LoopSpec, LoopState};
use crate::observe::EventBus;
use crate::queue::{Broker, Message};
use crate::scheduler::{PauseScope, Scheduler};
use crate::task::{Task, TaskId, TaskState};

use super::frame::PROTOCOL_VERSION;
use super::pagination::{Page, page_slice};

/// 队列容量上限（`queue.declare` 校验用）。
const MAX_QUEUE_CAPACITY: usize = 1_000_000;

/// 循环迭代次数上限（`loop.create` 校验用）。
const MAX_LOOP_ITERATIONS: f64 = 1_000_000.0;

/// 循环迭代间隔上限（毫秒）。
const MAX_LOOP_INTERVAL_MS: f64 = 86_400_000.0;

/// 循环迭代超时上限（毫秒）。
const MAX_LOOP_TIMEOUT_MS: f64 = 86_400_000.0;

/// 内核上下文：调度器 + 消息代理 + 事件总线（可选循环任务引擎）。
#[derive(Debug)]
pub struct Kernel {
    /// 调度器。
    pub scheduler: Arc<Scheduler>,
    /// 消息代理。
    pub broker: Arc<Broker>,
    /// 事件总线。
    pub events: Arc<EventBus>,
    /// 可选的循环任务引擎。
    loops: Option<Arc<LoopController>>,
    /// 可选的自动化规则引擎。
    rules: Option<Arc<RuleEngine>>,
}

impl Kernel {
    /// 组装内核上下文（同时把事件总线绑定到调度器与消息代理，使任务与消息事件可被推送）。
    pub fn new(scheduler: Arc<Scheduler>, broker: Arc<Broker>, events: Arc<EventBus>) -> Self {
        scheduler.set_events(Arc::clone(&events));
        broker.set_events(Arc::clone(&events));
        // 业务心跳：随心跳兜底回收过期租约，令「死消费者」的消息被重投（即使暂无新 lease）。
        let reclaim = Arc::clone(&broker);
        scheduler.add_tick_hook(Arc::new(move |_now| {
            let _ = reclaim.reclaim_expired();
        }));
        Self {
            scheduler,
            broker,
            events,
            loops: None,
            rules: None,
        }
    }

    /// 启用循环任务引擎：绑定事件总线，并把其 tick 挂到调度器心跳上。
    pub fn with_loops(mut self, loops: Arc<LoopController>) -> Self {
        loops.set_events(Arc::clone(&self.events));
        self.scheduler.add_tick_hook(loops.hook());
        self.loops = Some(loops);
        self
    }

    /// 启用自动化规则引擎：绑定事件总线与循环引擎，并把其 tick 挂到调度器心跳上。
    pub fn with_rules(mut self, rules: Arc<RuleEngine>) -> Self {
        rules.set_events(Arc::clone(&self.events));
        rules.set_loops(self.loops.clone());
        self.scheduler.add_tick_hook(rules.hook());
        self.rules = Some(rules);
        self
    }

    /// 访问已启用的循环任务引擎。
    fn loops_or_err(&self) -> std::result::Result<&Arc<LoopController>, Error> {
        self.loops
            .as_ref()
            .ok_or(Error::Protocol("loop engine is not enabled"))
    }

    /// 访问已启用的自动化规则引擎。
    fn rules_or_err(&self) -> std::result::Result<&Arc<RuleEngine>, Error> {
        self.rules
            .as_ref()
            .ok_or(Error::Protocol("automation rule engine is not enabled"))
    }

    /// 分发一个请求，返回响应载荷。
    pub fn handle(&self, request: &Value) -> Value {
        let name = request.get("m").and_then(Value::as_str).unwrap_or("");
        match name {
            "system.hello" => self.hello(request),
            "system.health" => self.health(),
            "heartbeat.get" => self.heartbeat(),
            "task.list" => self.task_list(request),
            "task.get" => self.task_get(request),
            "task.cancel" => self.task_cancel(request),
            "task.pause" => self.task_pause(request),
            "task.resume" => self.task_resume(request),
            "task.retry" => self.task_retry(request),
            "task.trigger" => self.task_trigger(request),
            "job.list" => self.job_list(request),
            "job.get" => self.job_get(request),
            "job.trigger" => self.job_trigger(request),
            "job.enable" => self.job_set_enabled(request, true),
            "job.disable" => self.job_set_enabled(request, false),
            "scheduler.pause" => self.scheduler_pause(request),
            "scheduler.resume" => self.scheduler_resume(request),
            "queue.list" => self.queue_list(),
            "queue.stats" => self.queue_stats(request),
            "queue.declare" => self.queue_declare(request),
            "queue.publish" => self.queue_publish(request),
            "queue.lease" => self.queue_lease(request),
            "queue.ack" => self.queue_ack(request),
            "queue.nack" => self.queue_nack(request),
            "loop.create" => self.loop_create(request),
            "loop.list" => self.loop_list(request),
            "loop.get" => self.loop_get(request),
            "loop.pause" => self.loop_pause(request),
            "loop.resume" => self.loop_resume(request),
            "loop.stop" => self.loop_stop(request),
            "loop.trigger" => self.loop_trigger(request),
            "automation.rule.create" => self.rule_create(request),
            "automation.rule.list" => self.rule_list(request),
            "automation.rule.get" => self.rule_get(request),
            "automation.rule.enable" => self.rule_set_enabled(request, true),
            "automation.rule.disable" => self.rule_set_enabled(request, false),
            "automation.rule.delete" => self.rule_delete(request),
            "metrics.get" => self.metrics_get(),
            "trace.get" => self.trace_get(request),
            other => fail(other, &Error::Protocol("unknown message")),
        }
    }

    fn hello(&self, request: &Value) -> Value {
        let version = request.get("ver").and_then(Value::as_f64).unwrap_or(0.0) as u8;
        if version != PROTOCOL_VERSION {
            return fail_code("system.hello", 1, "PROTO_VER", "protocol version mismatch");
        }
        let mut result = Value::object();
        result.insert("ver", Value::Number(f64::from(PROTOCOL_VERSION)));
        result.insert(
            "sessionId",
            Value::String(format!("s-{}", self.scheduler.now())),
        );
        result.insert("maxFrame", Value::Number(1_048_576.0));
        result.insert("maxInflight", Value::Number(64.0));
        result.insert("serverTimeMs", Value::Number(self.scheduler.now() as f64));
        let caps = Value::Array(vec![
            Value::String("query".to_string()),
            Value::String("control".to_string()),
            Value::String("stream".to_string()),
        ]);
        result.insert("caps", caps);
        ok("system.welcome", result)
    }

    fn health(&self) -> Value {
        let stats = self.scheduler.stats();
        let mut result = Value::object();
        result.insert("status", Value::String("ok".to_string()));
        result.insert(
            "version",
            Value::String(env!("CARGO_PKG_VERSION").to_string()),
        );
        result.insert("uptimeMs", Value::Number(self.scheduler.now() as f64));
        result.insert("sessions", Value::Number(1.0));
        result.insert("queues", Value::Number(self.broker.queues().len() as f64));
        result.insert("jobs", Value::Number(stats.jobs as f64));
        result.insert("paused", Value::Bool(stats.paused));
        ok("system.health", result)
    }

    fn heartbeat(&self) -> Value {
        let stats = self.scheduler.stats();
        let interval_ms = self.scheduler.heartbeat_interval_ms();
        let mut result = Value::object();
        result.insert("intervalMs", Value::Number(interval_ms as f64));
        result.insert(
            "nextTickMs",
            Value::Number((self.scheduler.now().saturating_add(interval_ms)) as f64),
        );
        result.insert("ticksTotal", Value::Number(stats.ticks as f64));
        result.insert(
            "lagMs",
            Value::Number(self.scheduler.heartbeat_lag_ms() as f64),
        );
        result.insert(
            "adaptive",
            Value::Bool(self.scheduler.is_heartbeat_adaptive()),
        );
        result.insert("ready", Value::Number(stats.ready as f64));
        result.insert("inflight", Value::Number(stats.inflight as f64));
        ok("heartbeat.get", result)
    }

    fn task_list(&self, request: &Value) -> Value {
        let page = Page::from_request(request, "createdAt");
        let state_filter = request
            .get("filter")
            .and_then(|filter| filter.get("state"))
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            });
        let rows: Vec<(u64, String, Value)> = self
            .scheduler
            .list()
            .iter()
            .filter(|task| {
                state_filter
                    .as_ref()
                    .is_none_or(|states| states.iter().any(|s| s == task.state.as_str()))
            })
            .map(|task| {
                let sort_value = match page.sort_by.as_str() {
                    "attempts" => u64::from(task.attempts),
                    _ => task.created_at.unwrap_or(0),
                };
                (sort_value, task.id.to_string(), task_to_value(task))
            })
            .collect();
        let (items, next_cursor) = page_slice(rows, &page);
        let mut result = Value::object();
        result.insert("items", Value::Array(items));
        result.insert("nextCursor", next_cursor.map_or(Value::Null, Value::String));
        ok("task.list", result)
    }

    fn task_get(&self, request: &Value) -> Value {
        let id = request.get("taskId").and_then(Value::as_str).unwrap_or("");
        match self.scheduler.get(&crate::task::TaskId::new(id)) {
            Some(task) => {
                let mut result = Value::object();
                result.insert("task", task_to_value(&task));
                ok("task.get", result)
            }
            None => fail(
                "task.get",
                &Error::NotFound {
                    kind: "task",
                    id: id.to_string(),
                },
            ),
        }
    }

    fn task_cancel(&self, request: &Value) -> Value {
        let id = request.get("taskId").and_then(Value::as_str).unwrap_or("");
        let mut result = Value::object();
        result.insert("taskId", Value::String(id.to_string()));
        match self.scheduler.cancel_task(&crate::task::TaskId::new(id)) {
            Ok(state) => {
                result.insert("state", Value::String(state.as_str().to_string()));
                ok("task.cancel", result)
            }
            Err(err) => fail("task.cancel", &err),
        }
    }

    /// 暂停任务（幂等）。
    fn task_pause(&self, request: &Value) -> Value {
        self.task_state_response("task.pause", request, Scheduler::pause_task)
    }

    /// 恢复任务（幂等）。
    fn task_resume(&self, request: &Value) -> Value {
        self.task_state_response("task.resume", request, Scheduler::resume_task)
    }

    /// 重试任务（`resetAttempts` 缺省 `false`）。
    fn task_retry(&self, request: &Value) -> Value {
        let reset = request
            .get("resetAttempts")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        self.task_state_response("task.retry", request, move |scheduler, id| {
            scheduler.retry_task(id, reset)
        })
    }

    /// `task.pause` / `task.resume` / `task.retry` 共用的状态响应（`{ taskId, state }`）。
    fn task_state_response(
        &self,
        name: &str,
        request: &Value,
        action: impl FnOnce(&Scheduler, &TaskId) -> crate::error::Result<TaskState>,
    ) -> Value {
        let id = request.get("taskId").and_then(Value::as_str).unwrap_or("");
        match action(&self.scheduler, &TaskId::new(id)) {
            Ok(state) => {
                let mut result = Value::object();
                result.insert("taskId", Value::String(id.to_string()));
                result.insert("state", Value::String(state.as_str().to_string()));
                ok(name, result)
            }
            Err(err) => fail(name, &err),
        }
    }

    fn task_trigger(&self, request: &Value) -> Value {
        // 指定 jobId 时按该定时任务的队列与名称触发
        if let Some(job_id) = request.get("jobId").and_then(Value::as_str) {
            return match self
                .scheduler
                .trigger_job(&crate::timer::JobId::new(job_id))
            {
                Ok(task_id) => task_id_response("task.trigger", &task_id),
                Err(err) => fail("task.trigger", &err),
            };
        }
        // 否则按显式 queue（可选 name）即时创建任务
        let Some(queue) = request.get("queue").and_then(Value::as_str) else {
            return fail(
                "task.trigger",
                &Error::Protocol("task.trigger requires jobId or queue"),
            );
        };
        let mut task = Task::new(queue);
        if let Some(name) = request.get("name").and_then(Value::as_str) {
            task = task.name(name);
        }
        match self.scheduler.submit(task) {
            Ok(task_id) => task_id_response("task.trigger", &task_id),
            Err(err) => fail("task.trigger", &err),
        }
    }

    fn scheduler_pause(&self, request: &Value) -> Value {
        self.scheduler.pause(pause_scope(request));
        self.pause_state("scheduler.pause")
    }

    fn scheduler_resume(&self, request: &Value) -> Value {
        self.scheduler.resume(pause_scope(request));
        self.pause_state("scheduler.resume")
    }

    fn pause_state(&self, name: &str) -> Value {
        let mut result = Value::object();
        result.insert("paused", Value::Bool(self.scheduler.is_paused()));
        result.insert("timers", Value::Bool(self.scheduler.is_timers_paused()));
        ok(name, result)
    }

    fn job_list(&self, request: &Value) -> Value {
        let page = Page::from_request(request, "nextRunAt");
        let rows: Vec<(u64, String, Value)> = self
            .scheduler
            .jobs()
            .iter()
            .map(|job| {
                let sort_value = match page.sort_by.as_str() {
                    "lastRunAt" => job.last_run_at.unwrap_or(0),
                    _ => job.next_run_at.unwrap_or(0),
                };
                (sort_value, job.id.to_string(), job_to_value(job))
            })
            .collect();
        let (items, next_cursor) = page_slice(rows, &page);
        let mut result = Value::object();
        result.insert("items", Value::Array(items));
        result.insert("nextCursor", next_cursor.map_or(Value::Null, Value::String));
        ok("job.list", result)
    }

    fn job_get(&self, request: &Value) -> Value {
        let id = request.get("jobId").and_then(Value::as_str).unwrap_or("");
        match self.scheduler.job(&crate::timer::JobId::new(id)) {
            Some(job) => {
                let mut result = Value::object();
                result.insert("job", job_to_value(&job));
                ok("job.get", result)
            }
            None => fail(
                "job.get",
                &Error::NotFound {
                    kind: "job",
                    id: id.to_string(),
                },
            ),
        }
    }

    fn job_trigger(&self, request: &Value) -> Value {
        let id = request.get("jobId").and_then(Value::as_str).unwrap_or("");
        match self.scheduler.trigger_job(&crate::timer::JobId::new(id)) {
            Ok(task_id) => {
                let mut result = Value::object();
                result.insert("taskId", Value::String(task_id.to_string()));
                ok("job.trigger", result)
            }
            Err(err) => fail("job.trigger", &err),
        }
    }

    /// `job.enable` / `job.disable` 共用实现。
    fn job_set_enabled(&self, request: &Value, enabled: bool) -> Value {
        let name = if enabled { "job.enable" } else { "job.disable" };
        let id = request.get("jobId").and_then(Value::as_str).unwrap_or("");
        let next = if enabled {
            crate::timer::JobState::Enabled
        } else {
            crate::timer::JobState::Disabled
        };
        match self
            .scheduler
            .set_job_state(&crate::timer::JobId::new(id), next)
        {
            Ok(()) => {
                let mut result = Value::object();
                result.insert("jobId", Value::String(id.to_string()));
                result.insert("state", Value::String(next.as_str().to_string()));
                ok(name, result)
            }
            Err(err) => fail(name, &err),
        }
    }

    fn queue_list(&self) -> Value {
        let queues = self.broker.queues();
        let items = queues
            .into_iter()
            .map(Value::String)
            .collect::<Vec<Value>>();
        let mut result = Value::object();
        result.insert("queues", Value::Array(items));
        ok("queue.list", result)
    }

    fn queue_stats(&self, request: &Value) -> Value {
        let names = match request.get("queue").and_then(Value::as_str) {
            Some(name) => vec![name.to_string()],
            None => self.broker.queues(),
        };
        let items = names
            .iter()
            .filter_map(|name| self.broker.stats(name))
            .map(|stat| {
                let mut value = Value::object();
                value.insert("name", Value::String(stat.name));
                value.insert("depth", Value::Number(stat.depth as f64));
                value.insert("inflight", Value::Number(stat.inflight as f64));
                value.insert("delivered", Value::Number(stat.delivered as f64));
                value.insert("acked", Value::Number(stat.acked as f64));
                value.insert("dead", Value::Number(stat.dead as f64));
                value.insert("ratePerSec", Value::Number(stat.rate_per_sec as f64));
                value
            })
            .collect::<Vec<Value>>();
        let mut result = Value::object();
        result.insert("queues", Value::Array(items));
        ok("queue.stats", result)
    }

    /// 声明队列（幂等：已存在的队列保持原容量）。
    fn queue_declare(&self, request: &Value) -> Value {
        let Some(queue) = request.get("queue").and_then(Value::as_str) else {
            return fail(
                "queue.declare",
                &Error::Protocol("queue.declare requires queue"),
            );
        };
        let capacity = match request.get("capacity") {
            Some(value) => {
                let Some(number) = value.as_f64() else {
                    return fail(
                        "queue.declare",
                        &Error::Protocol("queue.declare capacity must be a number"),
                    );
                };
                if !(1.0..=MAX_QUEUE_CAPACITY as f64).contains(&number) {
                    return fail(
                        "queue.declare",
                        &Error::Protocol("queue.declare capacity out of range"),
                    );
                }
                number as usize
            }
            None => self.broker.default_capacity(),
        };
        match self.broker.declare(queue, capacity) {
            Ok(()) => {
                let actual = self
                    .broker
                    .stats(queue)
                    .map_or(capacity, |stat| stat.capacity);
                let mut result = Value::object();
                result.insert("queue", Value::String(queue.to_string()));
                result.insert("capacity", Value::Number(actual as f64));
                ok("queue.declare", result)
            }
            Err(err) => fail("queue.declare", &err),
        }
    }

    /// 发布消息（`mode` 默认 `try`：队列满返回 `BUSY`；`block` 则阻塞等待空位）。
    fn queue_publish(&self, request: &Value) -> Value {
        let Some(queue) = request.get("queue").and_then(Value::as_str) else {
            return fail(
                "queue.publish",
                &Error::Protocol("queue.publish requires queue"),
            );
        };
        let Some(body) = request.get("body").and_then(Value::as_str) else {
            return fail(
                "queue.publish",
                &Error::Protocol("queue.publish requires body"),
            );
        };
        let blocking = matches!(request.get("mode").and_then(Value::as_str), Some("block"));
        let published = if blocking {
            self.broker.publish(queue, body)
        } else {
            self.broker.try_publish(queue, body)
        };
        match published {
            Ok(id) => {
                let mut result = Value::object();
                result.insert("msgId", Value::String(id.to_string()));
                result.insert("queue", Value::String(queue.to_string()));
                ok("queue.publish", result)
            }
            Err(err) => fail("queue.publish", &err),
        }
    }

    /// 租约一条消息；无可用消息时 `message` 为 `null`（`waitMs` 上限 30s）。
    fn queue_lease(&self, request: &Value) -> Value {
        let Some(queue) = request.get("queue").and_then(Value::as_str) else {
            return fail(
                "queue.lease",
                &Error::Protocol("queue.lease requires queue"),
            );
        };
        let wait_ms = request
            .get("waitMs")
            .and_then(Value::as_f64)
            .unwrap_or(0.0)
            .clamp(0.0, 30_000.0) as u64;
        let message = self.broker.lease(queue, Duration::from_millis(wait_ms));
        let mut result = Value::object();
        result.insert(
            "message",
            message.as_ref().map_or(Value::Null, message_to_value),
        );
        ok("queue.lease", result)
    }

    /// 确认消息（成功消费）。
    fn queue_ack(&self, request: &Value) -> Value {
        let Some(msg_id) = request.get("msgId").and_then(Value::as_str) else {
            return fail("queue.ack", &Error::Protocol("queue.ack requires msgId"));
        };
        match self.broker.ack(&crate::queue::MessageId::new(msg_id)) {
            Ok(()) => {
                let mut result = Value::object();
                result.insert("msgId", Value::String(msg_id.to_string()));
                result.insert("acked", Value::Bool(true));
                ok("queue.ack", result)
            }
            Err(err) => fail("queue.ack", &err),
        }
    }

    /// 否定确认：`requeue` 默认 `true`；返回 `dead` 表示是否进入死信。
    fn queue_nack(&self, request: &Value) -> Value {
        let Some(msg_id) = request.get("msgId").and_then(Value::as_str) else {
            return fail("queue.nack", &Error::Protocol("queue.nack requires msgId"));
        };
        let requeue = request
            .get("requeue")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        match self
            .broker
            .nack(&crate::queue::MessageId::new(msg_id), requeue)
        {
            Ok(dead) => {
                let mut result = Value::object();
                result.insert("msgId", Value::String(msg_id.to_string()));
                result.insert("dead", Value::Bool(dead));
                ok("queue.nack", result)
            }
            Err(err) => fail("queue.nack", &err),
        }
    }

    /// 创建循环任务。
    fn loop_create(&self, request: &Value) -> Value {
        let loops = match self.loops_or_err() {
            Ok(loops) => loops,
            Err(err) => return fail("loop.create", &err),
        };
        let Some(max_iterations) = request.get("maxIterations").and_then(Value::as_f64) else {
            return fail(
                "loop.create",
                &Error::Protocol("loop.create requires maxIterations"),
            );
        };
        if !(1.0..=MAX_LOOP_ITERATIONS).contains(&max_iterations) {
            return fail(
                "loop.create",
                &Error::Protocol("loop.create maxIterations out of range"),
            );
        }
        let queue = request
            .get("queue")
            .and_then(Value::as_str)
            .unwrap_or("loop");
        let mut spec = LoopSpec::new(queue).max_iterations(max_iterations as u64);
        if let Some(name) = request.get("name").and_then(Value::as_str) {
            spec = spec.name(name);
        }
        if let Some(value) = request.get("intervalMs") {
            let Some(number) = value.as_f64() else {
                return fail(
                    "loop.create",
                    &Error::Protocol("loop.create intervalMs must be a number"),
                );
            };
            if !(0.0..=MAX_LOOP_INTERVAL_MS).contains(&number) {
                return fail(
                    "loop.create",
                    &Error::Protocol("loop.create intervalMs out of range"),
                );
            }
            spec = spec.interval_ms(number as u64);
        }
        if let Some(backoff) = request.get("backoff").and_then(Value::as_bool) {
            spec = spec.backoff(backoff);
        }
        if let Some(value) = request.get("timeoutMs") {
            let Some(number) = value.as_f64() else {
                return fail(
                    "loop.create",
                    &Error::Protocol("loop.create timeoutMs must be a number"),
                );
            };
            if !(1.0..=MAX_LOOP_TIMEOUT_MS).contains(&number) {
                return fail(
                    "loop.create",
                    &Error::Protocol("loop.create timeoutMs out of range"),
                );
            }
            spec = spec.timeout_ms(number as u64);
        }
        if let Some(value) = request.get("deadlineMs") {
            let Some(number) = value.as_f64() else {
                return fail(
                    "loop.create",
                    &Error::Protocol("loop.create deadlineMs must be a number"),
                );
            };
            if number < 0.0 {
                return fail(
                    "loop.create",
                    &Error::Protocol("loop.create deadlineMs out of range"),
                );
            }
            spec = spec.deadline_ms(number as u64);
        }
        if let Some(policy) = request.get("onError").and_then(Value::as_str) {
            match policy.parse::<LoopOnError>() {
                Ok(policy) => spec = spec.on_error(policy),
                Err(err) => return fail("loop.create", &err),
            }
        }
        if let Some(key) = request.get("idempotencyKey").and_then(Value::as_str) {
            spec = spec.idempotency_key(key);
        }
        match loops.create(spec) {
            Ok(id) => {
                let state = loops.get(&id).map_or(LoopState::Pending, |item| item.state);
                let mut result = Value::object();
                result.insert("loopId", Value::String(id.to_string()));
                result.insert("state", Value::String(state.as_str().to_string()));
                ok("loop.create", result)
            }
            Err(err) => fail("loop.create", &err),
        }
    }

    /// 列出循环任务（游标分页）。
    fn loop_list(&self, request: &Value) -> Value {
        let loops = match self.loops_or_err() {
            Ok(loops) => loops,
            Err(err) => return fail("loop.list", &err),
        };
        let page = Page::from_request(request, "createdAt");
        let rows: Vec<(u64, String, Value)> = loops
            .list()
            .iter()
            .map(|item| {
                let sort_value = match page.sort_by.as_str() {
                    "iteration" => item.iteration,
                    _ => item.created_at,
                };
                (sort_value, item.id.to_string(), loop_to_value(item))
            })
            .collect();
        let (items, next_cursor) = page_slice(rows, &page);
        let mut result = Value::object();
        result.insert("items", Value::Array(items));
        result.insert("nextCursor", next_cursor.map_or(Value::Null, Value::String));
        ok("loop.list", result)
    }

    /// 查询单个循环任务。
    fn loop_get(&self, request: &Value) -> Value {
        let loops = match self.loops_or_err() {
            Ok(loops) => loops,
            Err(err) => return fail("loop.get", &err),
        };
        let id = request.get("loopId").and_then(Value::as_str).unwrap_or("");
        match loops.get(&LoopId::new(id)) {
            Some(item) => {
                let mut result = Value::object();
                result.insert("loop", loop_to_value(&item));
                ok("loop.get", result)
            }
            None => fail(
                "loop.get",
                &Error::NotFound {
                    kind: "loop",
                    id: id.to_string(),
                },
            ),
        }
    }

    /// `loop.pause` / `loop.resume` / `loop.stop` 共用的状态响应。
    fn loop_state_response(
        &self,
        name: &str,
        request: &Value,
        action: impl FnOnce(&LoopController, &LoopId) -> crate::error::Result<LoopState>,
    ) -> Value {
        let loops = match self.loops_or_err() {
            Ok(loops) => loops,
            Err(err) => return fail(name, &err),
        };
        let id = request.get("loopId").and_then(Value::as_str).unwrap_or("");
        let loop_id = LoopId::new(id);
        match action(loops, &loop_id) {
            Ok(state) => {
                let mut result = Value::object();
                result.insert("loopId", Value::String(id.to_string()));
                result.insert("state", Value::String(state.as_str().to_string()));
                ok(name, result)
            }
            Err(err) => fail(name, &err),
        }
    }

    fn loop_pause(&self, request: &Value) -> Value {
        self.loop_state_response("loop.pause", request, LoopController::pause)
    }

    fn loop_resume(&self, request: &Value) -> Value {
        self.loop_state_response("loop.resume", request, LoopController::resume)
    }

    fn loop_stop(&self, request: &Value) -> Value {
        self.loop_state_response("loop.stop", request, LoopController::stop)
    }

    /// 立即触发一次迭代，返回生成的迭代任务 ID。
    fn loop_trigger(&self, request: &Value) -> Value {
        let loops = match self.loops_or_err() {
            Ok(loops) => loops,
            Err(err) => return fail("loop.trigger", &err),
        };
        let id = request.get("loopId").and_then(Value::as_str).unwrap_or("");
        match loops.trigger(&LoopId::new(id)) {
            Ok(task_id) => task_id_response("loop.trigger", &task_id),
            Err(err) => fail("loop.trigger", &err),
        }
    }

    /// 创建自动化规则。
    fn rule_create(&self, request: &Value) -> Value {
        let rules = match self.rules_or_err() {
            Ok(rules) => rules,
            Err(err) => return fail("automation.rule.create", &err),
        };
        let Some(on) = request.get("on").and_then(Value::as_str) else {
            return fail(
                "automation.rule.create",
                &Error::Protocol("automation.rule.create requires on"),
            );
        };
        let Some(action_value) = request.get("action") else {
            return fail(
                "automation.rule.create",
                &Error::Protocol("automation.rule.create requires action"),
            );
        };
        let action = match RuleAction::from_value(action_value) {
            Ok(action) => action,
            Err(err) => return fail("automation.rule.create", &err),
        };
        let filter = match request.get("filter") {
            Some(value) => match RuleFilter::from_value(value) {
                Ok(filter) => filter,
                Err(err) => return fail("automation.rule.create", &err),
            },
            None => RuleFilter::default(),
        };
        let mut spec = RuleSpec::new(on, action).filter(filter);
        if let Some(name) = request.get("name").and_then(Value::as_str) {
            spec = spec.name(name);
        }
        if let Some(enabled) = request.get("enabled").and_then(Value::as_bool) {
            spec = spec.enabled(enabled);
        }
        if let Some(value) = request.get("maxFires") {
            let Some(number) = value.as_f64() else {
                return fail(
                    "automation.rule.create",
                    &Error::Protocol("automation.rule.create maxFires must be a number"),
                );
            };
            if !(1.0..=1_000_000_000.0).contains(&number) {
                return fail(
                    "automation.rule.create",
                    &Error::Protocol("automation.rule.create maxFires out of range"),
                );
            }
            spec = spec.max_fires(number as u64);
        }
        if let Some(key) = request.get("idempotencyKey").and_then(Value::as_str) {
            spec = spec.idempotency_key(key);
        }
        match rules.create(spec) {
            Ok(id) => {
                let enabled = rules.get(&id).is_none_or(|rule| rule.enabled);
                let mut result = Value::object();
                result.insert("ruleId", Value::String(id.to_string()));
                result.insert("enabled", Value::Bool(enabled));
                ok("automation.rule.create", result)
            }
            Err(err) => fail("automation.rule.create", &err),
        }
    }

    /// 列出自动化规则（游标分页）。
    fn rule_list(&self, request: &Value) -> Value {
        let rules = match self.rules_or_err() {
            Ok(rules) => rules,
            Err(err) => return fail("automation.rule.list", &err),
        };
        let page = Page::from_request(request, "createdAt");
        let rows: Vec<(u64, String, Value)> = rules
            .list()
            .iter()
            .map(|rule| {
                let sort_value = match page.sort_by.as_str() {
                    "fires" => rule.fires,
                    _ => rule.created_at,
                };
                (sort_value, rule.id.to_string(), rule_to_value(rule))
            })
            .collect();
        let (items, next_cursor) = page_slice(rows, &page);
        let mut result = Value::object();
        result.insert("items", Value::Array(items));
        result.insert("nextCursor", next_cursor.map_or(Value::Null, Value::String));
        ok("automation.rule.list", result)
    }

    /// 查询单条自动化规则。
    fn rule_get(&self, request: &Value) -> Value {
        let rules = match self.rules_or_err() {
            Ok(rules) => rules,
            Err(err) => return fail("automation.rule.get", &err),
        };
        let id = request.get("ruleId").and_then(Value::as_str).unwrap_or("");
        match rules.get(&RuleId::new(id)) {
            Some(rule) => {
                let mut result = Value::object();
                result.insert("rule", rule_to_value(&rule));
                ok("automation.rule.get", result)
            }
            None => fail(
                "automation.rule.get",
                &Error::NotFound {
                    kind: "rule",
                    id: id.to_string(),
                },
            ),
        }
    }

    /// `automation.rule.enable` / `automation.rule.disable` 共用实现。
    fn rule_set_enabled(&self, request: &Value, enabled: bool) -> Value {
        let name = if enabled {
            "automation.rule.enable"
        } else {
            "automation.rule.disable"
        };
        let rules = match self.rules_or_err() {
            Ok(rules) => rules,
            Err(err) => return fail(name, &err),
        };
        let id = request.get("ruleId").and_then(Value::as_str).unwrap_or("");
        match rules.set_enabled(&RuleId::new(id), enabled) {
            Ok(state) => {
                let mut result = Value::object();
                result.insert("ruleId", Value::String(id.to_string()));
                result.insert("enabled", Value::Bool(state));
                ok(name, result)
            }
            Err(err) => fail(name, &err),
        }
    }

    /// 删除自动化规则（幂等）。
    fn rule_delete(&self, request: &Value) -> Value {
        let rules = match self.rules_or_err() {
            Ok(rules) => rules,
            Err(err) => return fail("automation.rule.delete", &err),
        };
        let id = request.get("ruleId").and_then(Value::as_str).unwrap_or("");
        let deleted = rules.delete(&RuleId::new(id));
        let mut result = Value::object();
        result.insert("ruleId", Value::String(id.to_string()));
        result.insert("deleted", Value::Bool(deleted));
        ok("automation.rule.delete", result)
    }

    fn metrics_get(&self) -> Value {
        let stats = self.scheduler.stats();
        let mut counters = Value::object();
        counters.insert("submitted", Value::Number(stats.submitted as f64));
        counters.insert("succeeded", Value::Number(stats.succeeded as f64));
        counters.insert("failed", Value::Number(stats.failed as f64));
        counters.insert("retried", Value::Number(stats.retried as f64));
        counters.insert("dead", Value::Number(stats.dead as f64));
        counters.insert("timedOut", Value::Number(stats.timed_out as f64));
        counters.insert("jobFires", Value::Number(stats.job_fires as f64));
        counters.insert("throttled", Value::Number(stats.throttled as f64));
        counters.insert("ticks", Value::Number(stats.ticks as f64));
        counters.insert("ready", Value::Number(stats.ready as f64));
        counters.insert("inflight", Value::Number(stats.inflight as f64));
        let mut result = Value::object();
        result.insert("counters", counters);
        result.insert(
            "latestEventSeq",
            Value::Number(self.events.latest_seq() as f64),
        );
        ok("metrics.get", result)
    }

    fn trace_get(&self, request: &Value) -> Value {
        let id = request.get("taskId").and_then(Value::as_str).unwrap_or("");
        let Some(task) = self.scheduler.get(&crate::task::TaskId::new(id)) else {
            return fail(
                "trace.get",
                &Error::NotFound {
                    kind: "task",
                    id: id.to_string(),
                },
            );
        };
        let mut spans: Vec<Value> = Vec::new();
        // 1) 排队等待：创建 → 首次执行
        if let (Some(created), Some(first)) = (task.created_at, task.first_started_at) {
            spans.push(span_value(
                "queue_wait",
                created,
                first.saturating_sub(created),
                true,
                None,
            ));
        }
        // 2) 每一次执行尝试（run / retry）
        for span in &task.spans {
            spans.push(span_value(
                &span.name,
                span.start_ts,
                span.dur_ms,
                span.ok,
                span.detail.as_deref(),
            ));
        }
        // 3) 任务总览
        if let Some(created) = task.created_at {
            let end = task.finished_at.unwrap_or_else(|| self.scheduler.now());
            spans.push(span_value(
                "task",
                created,
                end.saturating_sub(created),
                task.state == TaskState::Succeeded,
                None,
            ));
        }
        let mut result = Value::object();
        result.insert("spans", Value::Array(spans));
        ok("trace.get", result)
    }
}

/// 构造 `{ taskId }` 形式的成功响应。
fn task_id_response(name: &str, task_id: &crate::task::TaskId) -> Value {
    let mut result = Value::object();
    result.insert("taskId", Value::String(task_id.to_string()));
    ok(name, result)
}

/// 构造一个 span 值。
fn span_value(name: &str, start_ts: u64, dur_ms: u64, ok: bool, detail: Option<&str>) -> Value {
    let mut span = Value::object();
    span.insert("name", Value::String(name.to_string()));
    span.insert("startTs", Value::Number(start_ts as f64));
    span.insert("durMs", Value::Number(dur_ms as f64));
    span.insert("ok", Value::Bool(ok));
    if let Some(detail) = detail {
        span.insert("detail", Value::String(detail.to_string()));
    }
    span
}

/// 解析暂停范围（缺省 `all`）。
fn pause_scope(request: &Value) -> PauseScope {
    match request.get("scope").and_then(Value::as_str) {
        Some("timers") => PauseScope::Timers,
        _ => PauseScope::All,
    }
}

/// 构造成功响应。
fn ok(name: &str, result: Value) -> Value {
    let mut value = Value::object();
    value.insert("m", Value::String(name.to_string()));
    value.insert("ok", Value::Bool(true));
    value.insert("result", result);
    value
}

/// 由错误构造失败响应。
fn fail(name: &str, error: &Error) -> Value {
    fail_code(name, error.code(), error.code_name(), &error.to_string())
}

/// 构造失败响应。
fn fail_code(name: &str, code: i32, code_name: &str, message: &str) -> Value {
    let mut detail = Value::object();
    detail.insert("code", Value::Number(f64::from(code)));
    detail.insert("codeName", Value::String(code_name.to_string()));
    detail.insert("message", Value::String(message.to_string()));
    let mut value = Value::object();
    value.insert("m", Value::String(name.to_string()));
    value.insert("ok", Value::Bool(false));
    value.insert("error", detail);
    value
}

fn task_to_value(task: &Task) -> Value {
    let mut value = Value::object();
    value.insert("id", Value::String(task.id.to_string()));
    if let Some(name) = &task.name {
        value.insert("name", Value::String(name.clone()));
    }
    value.insert("queue", Value::String(task.queue.clone()));
    value.insert("state", Value::String(task.state.as_str().to_string()));
    value.insert("priority", Value::Number(f64::from(task.priority.get())));
    value.insert("attempts", Value::Number(f64::from(task.attempts)));
    value.insert("maxAttempts", Value::Number(f64::from(task.max_attempts)));
    if let Some(created) = task.created_at {
        value.insert("createdAt", Value::Number(created as f64));
    }
    if let Some(started) = task.started_at {
        value.insert("startedAt", Value::Number(started as f64));
    }
    if let Some(finished) = task.finished_at {
        value.insert("finishedAt", Value::Number(finished as f64));
    }
    if let Some(error) = &task.last_error {
        value.insert("lastError", Value::String(error.clone()));
    }
    value
}

fn job_to_value(job: &crate::timer::Job) -> Value {
    let mut value = Value::object();
    value.insert("id", Value::String(job.id.to_string()));
    value.insert("name", Value::String(job.name.clone()));
    value.insert("queue", Value::String(job.queue.clone()));
    value.insert("state", Value::String(job.state.as_str().to_string()));
    value.insert(
        "misfirePolicy",
        Value::String(job.misfire.as_str().to_string()),
    );
    if let Some(next) = job.next_run_at {
        value.insert("nextRunAt", Value::Number(next as f64));
    }
    if let Some(last) = job.last_run_at {
        value.insert("lastRunAt", Value::Number(last as f64));
    }
    value.insert(
        "consecutiveFailures",
        Value::Number(f64::from(job.consecutive_failures)),
    );
    value.insert(
        "maxConsecutiveFailures",
        Value::Number(f64::from(job.max_consecutive_failures)),
    );
    value
}

fn message_to_value(message: &Message) -> Value {
    let mut value = Value::object();
    value.insert("id", Value::String(message.id.to_string()));
    value.insert("queue", Value::String(message.queue.clone()));
    value.insert("body", Value::String(message.body.clone()));
    value.insert("attempts", Value::Number(f64::from(message.attempts)));
    value.insert("createdAt", Value::Number(message.created_at as f64));
    value
}

fn loop_to_value(item: &Loop) -> Value {
    let mut value = Value::object();
    value.insert("id", Value::String(item.id.to_string()));
    if let Some(name) = &item.spec.name {
        value.insert("name", Value::String(name.clone()));
    }
    value.insert("queue", Value::String(item.spec.queue.clone()));
    value.insert("state", Value::String(item.state.as_str().to_string()));
    value.insert(
        "maxIterations",
        Value::Number(item.spec.max_iterations as f64),
    );
    value.insert("intervalMs", Value::Number(item.spec.interval_ms as f64));
    value.insert("backoff", Value::Bool(item.spec.backoff));
    value.insert(
        "onError",
        Value::String(item.spec.on_error.as_str().to_string()),
    );
    value.insert("iteration", Value::Number(item.iteration as f64));
    value.insert(
        "consecutiveFailures",
        Value::Number(f64::from(item.consecutive_failures)),
    );
    value.insert("createdAt", Value::Number(item.created_at as f64));
    value.insert("updatedAt", Value::Number(item.updated_at as f64));
    if let Some(started) = item.started_at {
        value.insert("startedAt", Value::Number(started as f64));
    }
    if let Some(next) = item.next_run_at {
        value.insert("nextRunAt", Value::Number(next as f64));
    }
    if let Some(result) = &item.last_result {
        value.insert("lastResult", Value::String(result.clone()));
    }
    if let Some(error) = &item.last_error {
        value.insert("lastError", Value::String(error.clone()));
    }
    if let Some(ms) = item.spec.timeout_ms {
        value.insert("timeoutMs", Value::Number(ms as f64));
    }
    if let Some(at) = item.spec.deadline_ms {
        value.insert("deadlineMs", Value::Number(at as f64));
    }
    value
}

fn rule_to_value(rule: &Rule) -> Value {
    let mut value = Value::object();
    value.insert("id", Value::String(rule.id.to_string()));
    if let Some(name) = &rule.name {
        value.insert("name", Value::String(name.clone()));
    }
    value.insert("on", Value::String(rule.on.clone()));
    value.insert("filter", rule.filter.to_value());
    value.insert("action", rule.action.to_value());
    value.insert("enabled", Value::Bool(rule.enabled));
    value.insert("fires", Value::Number(rule.fires as f64));
    if let Some(max) = rule.max_fires {
        value.insert("maxFires", Value::Number(max as f64));
    }
    value.insert("createdAt", Value::Number(rule.created_at as f64));
    value.insert("updatedAt", Value::Number(rule.updated_at as f64));
    value
}
