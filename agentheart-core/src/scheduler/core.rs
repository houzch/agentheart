//! 调度中心核心：任务提交、线程池执行、超时重试与定时任务。

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::clock::{Clock, SystemClock};
use crate::concurrency::idempotency::IdempotencyMap;
use crate::concurrency::random::Rng;
use crate::concurrency::rate_limit::TokenBucket;
use crate::concurrency::resource_lock::ResourceLocks;
use crate::concurrency::{FileLock, RateLimit};
use crate::error::{Error, Result};
use crate::json::Value;
use crate::observe::{EventBus, Metrics, log};
use crate::task::{Task, TaskId, TaskSpan, TaskState};
use crate::timer::{Job, JobId, JobState, MisfirePolicy};

use super::heartbeat::{Heartbeat, HeartbeatConfig, Load};
use super::queue::{ReadyEntry, TaskQueue};

/// 任务处理函数。
///
/// 由宿主注册；内核在任务执行时调用。可安全地被多线程并发调用。
pub type Handler = Arc<dyn Fn(&Task) -> Result<()> + Send + Sync + 'static>;

/// 由看门狗注入默认超时的任务标记。
const WATCHDOG_TAG: &str = "watchdog";

/// 心跳钩子：随每次心跳被调用，入参为当前 Unix 毫秒。
///
/// 用于把可选子系统（如循环任务控制器）挂到统一心跳上；
/// 钩子在**心跳线程**上执行，应保持轻量、不得阻塞。
pub type TickHook = Arc<dyn Fn(u64) + Send + Sync + 'static>;

/// 调度器配置。
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// 工作线程数（至少为 1）。
    pub workers: usize,
    /// 心跳基础间隔。
    pub heartbeat_interval: Duration,
    /// 心跳是否自适应（空闲拉长 / 繁忙缩短）。
    pub adaptive_heartbeat: bool,
    /// 空闲时的心跳拉长因子（`base * idle_factor`）。
    pub heartbeat_idle_factor: u32,
    /// 繁忙时的心跳缩短因子（`base / busy_factor`）。
    pub heartbeat_busy_factor: u32,
    /// 自适应心跳的间隔下限。
    pub heartbeat_min_interval: Duration,
    /// 自适应心跳的间隔上限。
    pub heartbeat_max_interval: Duration,
    /// 重试基础退避。
    pub retry_base: Duration,
    /// 重试最大退避。
    pub retry_max: Duration,
    /// 任务默认最大尝试次数（任务未指定时生效）。
    pub default_max_attempts: u32,
    /// 任务看门狗超时（业务心跳）：为未显式设置 `timeout` 的任务注入该默认超时，
    /// 并在任务最终因看门狗超时进入 `dead` 时投递 `event.error`；`None` 表示关闭。
    pub task_watchdog_timeout: Option<Duration>,
    /// 单次心跳最多补齐的错过触发次数（`CatchUp` 策略上限）。
    pub max_catch_up: u32,
    /// 令牌桶限流（`None` 表示不限流）。
    pub rate_limit: Option<RateLimit>,
    /// 重试退避是否加入随机抖动。
    pub jitter: bool,
    /// 幂等去重表容量。
    pub idempotency_capacity: usize,
    /// 选主锁文件路径（`None` 表示不参与选主）。
    pub leader_lock_path: Option<PathBuf>,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            workers: 4,
            heartbeat_interval: Duration::from_millis(500),
            adaptive_heartbeat: true,
            heartbeat_idle_factor: 4,
            heartbeat_busy_factor: 2,
            heartbeat_min_interval: Duration::from_millis(100),
            heartbeat_max_interval: Duration::from_secs(30),
            retry_base: Duration::from_millis(200),
            retry_max: Duration::from_secs(30),
            default_max_attempts: 3,
            task_watchdog_timeout: None,
            max_catch_up: 8,
            rate_limit: None,
            jitter: true,
            idempotency_capacity: 10_000,
            leader_lock_path: None,
        }
    }
}

/// 暂停范围。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PauseScope {
    /// 暂停任务执行与定时触发。
    All,
    /// 仅暂停定时触发（已在途与待执行的任务继续）。
    Timers,
}

/// 调度器运行统计（一次采样）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SchedulerStats {
    /// 就绪队列长度。
    pub ready: usize,
    /// 延时（重试）队列长度。
    pub delayed: usize,
    /// 正在执行的任务数。
    pub inflight: usize,
    /// 已注册的定时任务数。
    pub jobs: usize,
    /// 已完成的心跳次数。
    pub ticks: u64,
    /// 累计提交数。
    pub submitted: u64,
    /// 累计成功数。
    pub succeeded: u64,
    /// 累计失败数。
    pub failed: u64,
    /// 累计重试数。
    pub retried: u64,
    /// 累计死信数。
    pub dead: u64,
    /// 累计超时数。
    pub timed_out: u64,
    /// 累计定时任务触发数。
    pub job_fires: u64,
    /// 累计被限流数。
    pub throttled: u64,
    /// 任务执行是否已暂停。
    pub paused: bool,
    /// 定时触发是否已暂停。
    pub timers_paused: bool,
}

#[derive(Debug, Default)]
struct State {
    queue: TaskQueue,
    tasks: HashMap<TaskId, Task>,
    jobs: HashMap<JobId, Job>,
    seq: u64,
    inflight: usize,
}

struct Shared {
    state: Mutex<State>,
    ready_cv: Condvar,
    metrics: Metrics,
    heartbeat: Heartbeat,
    shutdown: AtomicBool,
    handler: Mutex<Option<Handler>>,
    clock: Arc<dyn Clock>,
    ticks: AtomicU64,
    paused: AtomicBool,
    timers_paused: AtomicBool,
    events: Mutex<Option<Arc<EventBus>>>,
    idempotency: Mutex<IdempotencyMap>,
    locks: ResourceLocks,
    rate: Mutex<Option<TokenBucket>>,
    rng: Mutex<Rng>,
    leader: Mutex<Option<FileLock>>,
    hooks: Mutex<Vec<TickHook>>,
    /// 任务看门狗超时（由配置注入；`None` 表示关闭）。
    watchdog: Option<Duration>,
}

/// 调度器：心跳驱动的任务调度与执行中心。
pub struct Scheduler {
    shared: Arc<Shared>,
    config: SchedulerConfig,
    workers: Mutex<Vec<JoinHandle<()>>>,
    heartbeat_thread: Mutex<Option<JoinHandle<()>>>,
}

impl fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scheduler")
            .field("config", &self.config)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl Scheduler {
    /// 创建调度器（使用系统时钟，尚未启动线程）。
    pub fn new(config: SchedulerConfig) -> Self {
        Self::with_clock(config, Arc::new(SystemClock))
    }

    /// 使用指定时钟创建调度器（便于测试注入确定时间）。
    pub fn with_clock(config: SchedulerConfig, clock: Arc<dyn Clock>) -> Self {
        let heartbeat = Heartbeat::new(HeartbeatConfig {
            interval: config.heartbeat_interval,
            adaptive: config.adaptive_heartbeat,
            idle_factor: config.heartbeat_idle_factor,
            busy_factor: config.heartbeat_busy_factor,
            min_interval: config.heartbeat_min_interval,
            max_interval: config.heartbeat_max_interval,
        });
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            ready_cv: Condvar::new(),
            metrics: Metrics::default(),
            heartbeat,
            shutdown: AtomicBool::new(false),
            handler: Mutex::new(None),
            clock,
            ticks: AtomicU64::new(0),
            paused: AtomicBool::new(false),
            timers_paused: AtomicBool::new(false),
            events: Mutex::new(None),
            idempotency: Mutex::new(IdempotencyMap::new(config.idempotency_capacity)),
            locks: ResourceLocks::default(),
            rate: Mutex::new(config.rate_limit.map(TokenBucket::new)),
            rng: Mutex::new(Rng::from_entropy()),
            leader: Mutex::new(None),
            hooks: Mutex::new(Vec::new()),
            watchdog: config.task_watchdog_timeout,
        });
        Self {
            shared,
            config,
            workers: Mutex::new(Vec::new()),
            heartbeat_thread: Mutex::new(None),
        }
    }

    /// 使用系统时钟创建并立即启动。
    pub fn start_with(config: SchedulerConfig, handler: Handler) -> Self {
        let scheduler = Self::new(config);
        scheduler.start(handler);
        scheduler
    }

    /// 设置 / 替换任务处理函数。
    pub fn set_handler(&self, handler: Handler) {
        *lock(&self.shared.handler) = Some(handler);
    }

    /// 绑定事件总线：此后内核会投递任务生命周期事件（`event.task`）。
    pub fn set_events(&self, events: Arc<EventBus>) {
        *lock(&self.shared.events) = Some(events);
    }

    /// 绑定事件总线（builder 风格）。
    pub fn with_events(self, events: Arc<EventBus>) -> Self {
        self.set_events(events);
        self
    }

    /// 注册一个心跳钩子：每次心跳（含手动 [`Scheduler::tick`]）都会以当前 Unix 毫秒调用。
    ///
    /// 钩子在心跳线程上执行，须保持轻量、不得阻塞或长时间持锁。
    pub fn add_tick_hook(&self, hook: TickHook) {
        lock(&self.shared.hooks).push(hook);
    }

    /// 启动工作线程与心跳线程（重复调用无副作用）。
    pub fn start(&self, handler: Handler) {
        self.set_handler(handler);
        self.acquire_leader_lock();
        let mut workers = lock(&self.workers);
        if !workers.is_empty() {
            return;
        }
        for index in 0..self.config.workers.max(1) {
            let shared = Arc::clone(&self.shared);
            let config = self.config.clone();
            let spawned = thread::Builder::new()
                .name(format!("agentheart-worker-{index}"))
                .spawn(move || worker_loop(&shared, &config));
            match spawned {
                Ok(handle) => workers.push(handle),
                Err(err) => {
                    log::error(&format!("启动工作线程失败: {err}"));
                    emit_error(&self.shared, &Error::Io(err), Some("worker"));
                }
            }
        }
        drop(workers);

        let shared = Arc::clone(&self.shared);
        let config = self.config.clone();
        let spawned = thread::Builder::new()
            .name("agentheart-heartbeat".to_string())
            .spawn(move || heartbeat_loop(&shared, &config));
        match spawned {
            Ok(handle) => *lock(&self.heartbeat_thread) = Some(handle),
            Err(err) => {
                log::error(&format!("启动心跳线程失败: {err}"));
                emit_error(&self.shared, &Error::Io(err), Some("heartbeat"));
            }
        }
    }

    /// 当前时间（Unix 毫秒，源自注入的时钟）。
    pub fn now(&self) -> u64 {
        self.shared.clock.now_ms()
    }

    /// 是否持有选主锁（未配置选主时始终为 `false`）。
    pub fn is_leader(&self) -> bool {
        lock(&self.shared.leader).is_some()
    }

    /// 当前生效的心跳间隔（毫秒，随自适应负载变化）。
    pub fn heartbeat_interval_ms(&self) -> u64 {
        self.shared.heartbeat.interval_ms()
    }

    /// 是否启用自适应心跳。
    pub fn is_heartbeat_adaptive(&self) -> bool {
        self.shared.heartbeat.is_adaptive()
    }

    /// 最近一次心跳的滞后（毫秒）。
    pub fn heartbeat_lag_ms(&self) -> u64 {
        self.shared.heartbeat.lag_ms()
    }

    fn acquire_leader_lock(&self) {
        let Some(path) = self.config.leader_lock_path.clone() else {
            return;
        };
        let reference = path.display().to_string();
        match FileLock::try_acquire(path) {
            Ok(Some(file_lock)) => {
                *lock(&self.shared.leader) = Some(file_lock);
                log::info("已获取选主锁");
            }
            Ok(None) => {
                log::warn("未获取选主锁：已有实例运行");
                emit_error(
                    &self.shared,
                    &Error::Conflict {
                        resource: reference.clone(),
                    },
                    Some(reference.as_str()),
                );
            }
            Err(err) => {
                log::error(&format!("选主锁获取失败: {err}"));
                emit_error(&self.shared, &err, Some(reference.as_str()));
            }
        }
    }

    /// 提交任务，返回任务 ID。
    ///
    /// # Errors
    /// 当任务状态不是 `Pending` 时返回 [`Error::Protocol`]。
    pub fn submit(&self, mut task: Task) -> Result<TaskId> {
        if task.state != TaskState::Pending {
            return Err(Error::Protocol("task must be pending when submitted"));
        }
        let idempotency = task.idempotency_key.clone();
        if let Some(key) = &idempotency {
            if let Some(existing) = lock(&self.shared.idempotency).get(key) {
                return Ok(existing.clone());
            }
        }
        if task.max_attempts == 0 {
            task.max_attempts = self.config.default_max_attempts.max(1);
        }
        task.created_at = Some(self.now());
        let id = self.enqueue(task);
        if let Some(key) = idempotency {
            lock(&self.shared.idempotency).insert(key, id.clone());
        }
        Ok(id)
    }

    /// 查询任务快照。
    pub fn get(&self, id: &TaskId) -> Option<Task> {
        lock(&self.shared.state).tasks.get(id).cloned()
    }

    /// 取消任务（幂等）；返回取消后的状态。
    ///
    /// # Errors
    /// 任务不存在时返回 [`Error::NotFound`]。
    pub fn cancel_task(&self, id: &TaskId) -> Result<TaskState> {
        let mut guard = lock(&self.shared.state);
        let task = guard.tasks.get_mut(id).ok_or_else(|| Error::NotFound {
            kind: "task",
            id: id.to_string(),
        })?;
        task.cancel()?;
        let state = task.state;
        drop(guard);
        emit(&self.shared, id, state, None, None);
        Ok(state)
    }

    /// 列出全部任务快照。
    pub fn list(&self) -> Vec<Task> {
        lock(&self.shared.state).tasks.values().cloned().collect()
    }

    /// 暂停任务（幂等）：仅 `pending` 可暂停并从队列摘除；执行中返回 [`Error::Conflict`]，
    /// 已结束的任务同样返回 [`Error::Conflict`]。
    ///
    /// # Errors
    /// 任务不存在时返回 [`Error::NotFound`]。
    pub fn pause_task(&self, id: &TaskId) -> Result<TaskState> {
        let state = {
            let mut guard = lock(&self.shared.state);
            let task = guard.tasks.get_mut(id).ok_or_else(|| Error::NotFound {
                kind: "task",
                id: id.to_string(),
            })?;
            match task.state {
                TaskState::Paused => return Ok(TaskState::Paused),
                TaskState::Pending => task.transition(TaskState::Paused)?,
                _ => {
                    return Err(Error::Conflict {
                        resource: id.to_string(),
                    });
                }
            }
            guard.queue.remove(id);
            TaskState::Paused
        };
        emit(&self.shared, id, state, None, None);
        Ok(state)
    }

    /// 恢复已暂停的任务（幂等）：重新入队并转回 `pending`；非暂停状态原样返回。
    ///
    /// # Errors
    /// 任务不存在时返回 [`Error::NotFound`]。
    pub fn resume_task(&self, id: &TaskId) -> Result<TaskState> {
        let state = {
            let mut guard = lock(&self.shared.state);
            let task = guard.tasks.get_mut(id).ok_or_else(|| Error::NotFound {
                kind: "task",
                id: id.to_string(),
            })?;
            if task.state != TaskState::Paused {
                return Ok(task.state);
            }
            task.transition(TaskState::Pending)?;
            let priority = task.priority;
            guard.seq += 1;
            let seq = guard.seq;
            guard
                .queue
                .push_ready(ReadyEntry::new(priority, seq, id.clone()));
            TaskState::Pending
        };
        self.shared.ready_cv.notify_one();
        emit(&self.shared, id, TaskState::Pending, None, None);
        Ok(state)
    }

    /// 重试任务：`dead` 任务重新入队为 `pending`（`reset_attempts` 为真时清零已尝试次数）。
    ///
    /// 幂等：`pending` / `retrying` 原样返回；执行中或已成功 / 已取消返回 [`Error::Conflict`]。
    ///
    /// # Errors
    /// 任务不存在时返回 [`Error::NotFound`]。
    pub fn retry_task(&self, id: &TaskId, reset_attempts: bool) -> Result<TaskState> {
        let state = {
            let mut guard = lock(&self.shared.state);
            let task = guard.tasks.get_mut(id).ok_or_else(|| Error::NotFound {
                kind: "task",
                id: id.to_string(),
            })?;
            let priority = match task.state {
                TaskState::Pending | TaskState::Retrying => return Ok(task.state),
                TaskState::Dead => {
                    task.transition(TaskState::Retrying)?;
                    task.transition(TaskState::Pending)?;
                    task.last_error = None;
                    task.next_retry_at = None;
                    task.finished_at = None;
                    if reset_attempts {
                        task.attempts = 0;
                    }
                    task.priority
                }
                _ => {
                    return Err(Error::Conflict {
                        resource: id.to_string(),
                    });
                }
            };
            guard.queue.remove(id);
            guard.seq += 1;
            let seq = guard.seq;
            guard
                .queue
                .push_ready(ReadyEntry::new(priority, seq, id.clone()));
            TaskState::Pending
        };
        self.shared.metrics.inc_retried();
        self.shared.ready_cv.notify_one();
        emit(&self.shared, id, TaskState::Pending, None, None);
        Ok(state)
    }

    /// 当前运行统计。
    pub fn stats(&self) -> SchedulerStats {
        let state = lock(&self.shared.state);
        let metrics = self.shared.metrics.snapshot();
        SchedulerStats {
            ready: state.queue.ready_len(),
            delayed: state.queue.delayed_len(),
            inflight: state.inflight,
            jobs: state.jobs.len(),
            ticks: self.shared.ticks.load(Ordering::Relaxed),
            submitted: metrics.submitted,
            succeeded: metrics.succeeded,
            failed: metrics.failed,
            retried: metrics.retried,
            dead: metrics.dead,
            timed_out: metrics.timed_out,
            job_fires: metrics.jobs_fired,
            throttled: metrics.throttled,
            paused: self.is_paused(),
            timers_paused: self.is_timers_paused(),
        }
    }

    // ---- 暂停 / 恢复 ----

    /// 暂停调度：`All` 同时暂停任务执行与定时触发；`Timers` 仅暂停定时触发。
    pub fn pause(&self, scope: PauseScope) {
        self.shared.timers_paused.store(true, Ordering::Release);
        if scope == PauseScope::All {
            self.shared.paused.store(true, Ordering::Release);
        }
    }

    /// 恢复调度（与 [`Scheduler::pause`] 的 `scope` 对应）。
    pub fn resume(&self, scope: PauseScope) {
        if scope == PauseScope::All {
            self.shared.paused.store(false, Ordering::Release);
        }
        self.shared.timers_paused.store(false, Ordering::Release);
        // 唤醒等待中的工作线程与心跳线程
        self.shared.ready_cv.notify_all();
        self.shared.heartbeat.wake();
    }

    /// 任务执行是否已暂停。
    pub fn is_paused(&self) -> bool {
        self.shared.paused.load(Ordering::Acquire)
    }

    /// 定时触发是否已暂停。
    pub fn is_timers_paused(&self) -> bool {
        self.shared.timers_paused.load(Ordering::Acquire)
    }

    // ---- 定时任务 ----

    /// 注册定时任务，返回任务 ID。
    pub fn add_job(&self, mut job: Job) -> JobId {
        job.refresh_next(self.now());
        let id = job.id.clone();
        lock(&self.shared.state).jobs.insert(id.clone(), job);
        id
    }

    /// 查询定时任务快照。
    pub fn job(&self, id: &JobId) -> Option<Job> {
        lock(&self.shared.state).jobs.get(id).cloned()
    }

    /// 列出全部定时任务快照。
    pub fn jobs(&self) -> Vec<Job> {
        lock(&self.shared.state).jobs.values().cloned().collect()
    }

    /// 启用 / 停用定时任务。
    ///
    /// # Errors
    /// 任务不存在时返回 [`Error::NotFound`]。
    pub fn set_job_state(&self, id: &JobId, next: JobState) -> Result<()> {
        let now = self.now();
        let mut guard = lock(&self.shared.state);
        let job = guard.jobs.get_mut(id).ok_or_else(|| Error::NotFound {
            kind: "job",
            id: id.to_string(),
        })?;
        job.state = next;
        if next == JobState::Enabled {
            job.refresh_next(now);
            // 重新启用时重置连续失败计数
            job.consecutive_failures = 0;
        }
        Ok(())
    }

    /// 移除定时任务。
    pub fn remove_job(&self, id: &JobId) -> Option<Job> {
        lock(&self.shared.state).jobs.remove(id)
    }

    /// 立即触发一次定时任务，返回生成的任务 ID。
    ///
    /// # Errors
    /// 任务不存在时返回 [`Error::NotFound`]。
    pub fn trigger_job(&self, id: &JobId) -> Result<TaskId> {
        let (queue, name, max_attempts) = {
            let guard = lock(&self.shared.state);
            let job = guard.jobs.get(id).ok_or_else(|| Error::NotFound {
                kind: "job",
                id: id.to_string(),
            })?;
            (job.queue.clone(), job.name.clone(), job.max_attempts)
        };
        let task_id = self.enqueue(
            Task::new(queue)
                .name(name)
                .max_attempts(max_attempts)
                .tag(format!("job:{id}")),
        );
        let now = self.now();
        if let Some(job) = lock(&self.shared.state).jobs.get_mut(id) {
            job.last_task_id = Some(task_id.clone());
            job.last_run_at = Some(now);
        }
        Ok(task_id)
    }

    /// 执行一次心跳（推进到期重试、触发到期定时任务、刷新自适应间隔）。
    ///
    /// 心跳线程会周期性调用同一逻辑；测试可结合 [`crate::clock::ManualClock`] 手动调用本方法。
    pub fn tick(&self) {
        heartbeat_tick(&self.shared, &self.config);
    }

    /// 关闭调度器并等待线程退出（幂等）。
    pub fn shutdown(&self) {
        if self.shared.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        self.shared.heartbeat.request_shutdown();
        self.shared.ready_cv.notify_all();
        {
            let mut workers = lock(&self.workers);
            for handle in workers.drain(..) {
                if handle.join().is_err() {
                    log::warn("工作线程异常退出");
                }
            }
        }
        if let Some(handle) = lock(&self.heartbeat_thread).take() {
            if handle.join().is_err() {
                log::warn("心跳线程异常退出");
            }
        }
    }

    fn enqueue(&self, task: Task) -> TaskId {
        enqueue(&self.shared, task)
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn worker_loop(shared: &Arc<Shared>, config: &SchedulerConfig) {
    while let Some(entry) = next_ready(shared) {
        let id = entry.id().clone();
        if !acquire_token(shared, config) {
            shared.metrics.inc_throttled();
            requeue_later(shared, &id, 50);
            continue;
        }
        let Some(task) = take_for_run(shared, &id) else {
            continue;
        };
        shared.metrics.inc_started();
        emit(shared, &id, TaskState::Running, Some(task.attempts), None);
        // 资源键互斥：同一键的任务串行执行（会话亲和）
        let resource = task.key.as_ref().map(|key| shared.locks.lock_for(key));
        let resource_guard = resource
            .as_ref()
            .map(|mutex| mutex.lock().unwrap_or_else(PoisonError::into_inner));
        let result = execute(shared, &task);
        drop(resource_guard);
        finish(shared, config, &task, result);
    }
}

fn acquire_token(shared: &Arc<Shared>, config: &SchedulerConfig) -> bool {
    if config.rate_limit.is_none() {
        return true;
    }
    let now = shared.clock.now_ms();
    let mut guard = lock(&shared.rate);
    match guard.as_mut() {
        Some(bucket) => bucket.try_acquire(now),
        None => true,
    }
}

fn requeue_later(shared: &Arc<Shared>, id: &TaskId, delay_ms: u64) {
    let now = shared.clock.now_ms();
    let mut state = lock(&shared.state);
    state
        .queue
        .push_delayed(now.saturating_add(delay_ms), id.clone());
}

fn next_ready(shared: &Shared) -> Option<ReadyEntry> {
    let mut guard = lock(&shared.state);
    loop {
        if !shared.paused.load(Ordering::Acquire) {
            if let Some(entry) = guard.queue.pop_ready() {
                return Some(entry);
            }
        }
        if shared.shutdown.load(Ordering::Acquire) {
            return None;
        }
        let (next, _) = shared
            .ready_cv
            .wait_timeout(guard, Duration::from_millis(200))
            .unwrap_or_else(PoisonError::into_inner);
        guard = next;
    }
}

fn take_for_run(shared: &Shared, id: &TaskId) -> Option<Task> {
    let mut guard = lock(&shared.state);
    let state: &mut State = &mut guard;
    let snapshot = {
        let task = state.tasks.get_mut(id)?;
        if matches!(task.state, TaskState::Paused | TaskState::Canceled) {
            return None;
        }
        if let Err(err) = task.transition(TaskState::Running) {
            log::warn(&format!("任务 {id} 无法进入运行态: {err}"));
            return None;
        }
        task.attempts = task.attempts.saturating_add(1);
        let now = shared.clock.now_ms();
        task.started_at = Some(now);
        if task.first_started_at.is_none() {
            task.first_started_at = Some(now);
        }
        task.clone()
    };
    state.inflight += 1;
    Some(snapshot)
}

fn execute(shared: &Arc<Shared>, task: &Task) -> Result<()> {
    let handler = lock(&shared.handler).clone();
    let Some(handler) = handler else {
        return Ok(());
    };
    match task.timeout {
        None => handler(task),
        Some(limit) => run_with_timeout(shared, handler, task, limit),
    }
}

fn run_with_timeout(
    shared: &Arc<Shared>,
    handler: Handler,
    task: &Task,
    limit: Duration,
) -> Result<()> {
    let (sender, receiver) = mpsc::channel();
    let owned = task.clone();
    let spawned = thread::Builder::new()
        .name("agentheart-task".to_string())
        .spawn(move || {
            let result = handler(&owned);
            if sender.send(result).is_err() {
                log::debug("任务结果无人接收（执行已超时）");
            }
        });
    let handle = match spawned {
        Ok(handle) => handle,
        Err(err) => {
            let error = Error::Io(err);
            emit_error(shared, &error, Some(task.id.as_str()));
            return Err(error);
        }
    };
    match receiver.recv_timeout(limit) {
        Ok(result) => {
            if handle.join().is_err() {
                log::warn("任务线程 panic");
                emit_error(
                    shared,
                    &Error::Internal("task thread panicked"),
                    Some(task.id.as_str()),
                );
            }
            result
        }
        Err(RecvTimeoutError::Timeout) => Err(Error::Timeout {
            id: task.id.to_string(),
        }),
        Err(RecvTimeoutError::Disconnected) => {
            if handle.join().is_err() {
                log::warn("任务线程 panic");
                emit_error(
                    shared,
                    &Error::Internal("task thread panicked"),
                    Some(task.id.as_str()),
                );
            }
            Err(Error::Internal("task thread disconnected"))
        }
    }
}

fn finish(shared: &Arc<Shared>, config: &SchedulerConfig, task: &Task, result: Result<()>) {
    let id = task.id.clone();
    let timed_out = matches!(&result, Err(Error::Timeout { .. }));
    let watchdog_aborted = timed_out && task.tags.iter().any(|tag| tag == WATCHDOG_TAG);
    let now = shared.clock.now_ms();
    let mut guard = lock(&shared.state);
    let state: &mut State = &mut guard;
    state.inflight = state.inflight.saturating_sub(1);
    let Some(record) = state.tasks.get_mut(&id) else {
        return;
    };
    // 记录本次尝试的链路跨度
    let started = record.started_at.unwrap_or(now);
    record.push_span(TaskSpan {
        name: if record.attempts > 1 {
            "retry".to_string()
        } else {
            "run".to_string()
        },
        start_ts: started,
        dur_ms: now.saturating_sub(started),
        ok: result.is_ok(),
        detail: match &result {
            Ok(()) => None,
            Err(err) => Some(err.to_string()),
        },
    });
    let (final_state, final_error) = match result {
        Ok(()) => {
            set_state(record, TaskState::Succeeded);
            record.finished_at = Some(now);
            shared.metrics.inc_succeeded();
            (TaskState::Succeeded, None)
        }
        Err(err) => {
            let text = err.to_string();
            record.last_error = Some(text.clone());
            set_state(record, TaskState::Failed);
            shared.metrics.inc_failed();
            if timed_out {
                shared.metrics.inc_timed_out();
            }
            if record.attempts >= record.max_attempts {
                record.finished_at = Some(now);
                set_state(record, TaskState::Dead);
                shared.metrics.inc_dead();
                (TaskState::Dead, Some(text))
            } else {
                set_state(record, TaskState::Retrying);
                let delay = backoff(shared, config, record.attempts);
                let due_ms = now.saturating_add(millis(delay));
                record.next_retry_at = Some(due_ms);
                state.queue.push_delayed(due_ms, id.clone());
                shared.metrics.inc_retried();
                (TaskState::Retrying, Some(text))
            }
        }
    };
    let attempt = record.attempts;
    drop(guard);
    emit(shared, &id, final_state, Some(attempt), final_error);
    attribute_job(shared, task, final_state);
    if final_state == TaskState::Dead && watchdog_aborted {
        emit_watchdog(shared, task);
    }
}

/// 投递「任务卡死」告警（`event.error`）：任务因看门狗超时最终进入 `dead`。
fn emit_watchdog(shared: &Shared, task: &Task) {
    let bus = lock(&shared.events).clone();
    let Some(bus) = bus else {
        return;
    };
    let limit = task.timeout.map_or(0, |limit| {
        u64::try_from(limit.as_millis()).unwrap_or(u64::MAX)
    });
    let message = format!(
        "task {} stuck: aborted by watchdog after {limit}ms per attempt",
        task.id
    );
    bus.publish_error(
        shared.clock.now_ms(),
        10,
        "INTERNAL",
        &message,
        Some(task.id.as_str()),
    );
}

/// 节律守护：把定时任务所触发任务的终态归属回其 Job，维护连续失败计数并在达阈值时告警。
///
/// 单次失败**不中断节律**（Job 继续按节律触发）；仅当连续失败次数达到
/// `max_consecutive_failures` 的整数倍时投递一次 `event.error`（`ref` 为该 Job ID）。
fn attribute_job(shared: &Arc<Shared>, task: &Task, state: TaskState) {
    if !matches!(state, TaskState::Succeeded | TaskState::Dead) {
        return;
    }
    let Some(job_id) = task
        .tags
        .iter()
        .find_map(|tag| tag.strip_prefix("job:"))
        .map(JobId::new)
    else {
        return;
    };
    let snapshot = {
        let mut guard = lock(&shared.state);
        let Some(job) = guard.jobs.get_mut(&job_id) else {
            return;
        };
        if state == TaskState::Succeeded {
            job.consecutive_failures = 0;
            return;
        }
        job.consecutive_failures = job.consecutive_failures.saturating_add(1);
        let threshold = job.max_consecutive_failures.max(1);
        if job.consecutive_failures % threshold != 0 {
            return;
        }
        job.clone()
    };
    emit_job_failure(shared, &snapshot);
}

/// 投递「定时任务连续失败」告警（`event.error`）。
fn emit_job_failure(shared: &Shared, job: &Job) {
    let bus = lock(&shared.events).clone();
    let Some(bus) = bus else {
        return;
    };
    let message = format!(
        "job {} failed {} consecutive times",
        job.id, job.consecutive_failures
    );
    bus.publish_error(
        shared.clock.now_ms(),
        10,
        "INTERNAL",
        &message,
        Some(job.id.as_str()),
    );
}

fn heartbeat_loop(shared: &Arc<Shared>, config: &SchedulerConfig) {
    while shared.heartbeat.wait_next() {
        heartbeat_tick(shared, config);
    }
}

/// 执行一次心跳：推进到期重试、触发到期定时任务、刷新自适应间隔并投递心跳事件。
fn heartbeat_tick(shared: &Arc<Shared>, config: &SchedulerConfig) {
    promote_due_retries(shared);
    let timers_due = fire_due_jobs(shared, config);
    update_load(shared);
    let tick = shared
        .ticks
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    emit_heartbeat(shared, tick, timers_due);
    run_tick_hooks(shared);
}

/// 依次调用已注册的心跳钩子（在锁外调用，避免阻塞心跳）。
fn run_tick_hooks(shared: &Shared) {
    let hooks: Vec<TickHook> = lock(&shared.hooks).clone();
    if hooks.is_empty() {
        return;
    }
    let now = shared.clock.now_ms();
    for hook in hooks {
        hook(now);
    }
}

/// 投递一条心跳事件（`event.heartbeat`）。
fn emit_heartbeat(shared: &Shared, tick: u64, timers_due: usize) {
    let bus = lock(&shared.events).clone();
    let Some(bus) = bus else {
        return;
    };
    let data = vec![
        ("tick".to_string(), Value::Number(tick as f64)),
        (
            "lagMs".to_string(),
            Value::Number(shared.heartbeat.lag_ms() as f64),
        ),
        ("timersDue".to_string(), Value::Number(timers_due as f64)),
    ];
    bus.publish(shared.clock.now_ms(), "heartbeat", data);
}

fn promote_due_retries(shared: &Arc<Shared>) -> usize {
    let now = shared.clock.now_ms();
    let mut promoted = 0_usize;
    {
        let mut guard = lock(&shared.state);
        let state: &mut State = &mut guard;
        let due = state.queue.take_due(now);
        for id in due {
            let priority = match state.tasks.get_mut(&id) {
                Some(record) => {
                    if record.state != TaskState::Pending {
                        set_state(record, TaskState::Pending);
                    }
                    record.priority
                }
                None => continue,
            };
            state.seq += 1;
            let seq = state.seq;
            state.queue.push_ready(ReadyEntry::new(priority, seq, id));
            promoted += 1;
        }
    }
    if promoted > 0 {
        shared.ready_cv.notify_all();
    }
    promoted
}

fn fire_due_jobs(shared: &Arc<Shared>, config: &SchedulerConfig) -> usize {
    if shared.timers_paused.load(Ordering::Acquire) {
        return 0;
    }
    let now = shared.clock.now_ms();
    let max_catch_up = config.max_catch_up;
    let mut spawns: Vec<(JobId, String, String, u32)> = Vec::new();
    {
        let mut guard = lock(&shared.state);
        let state: &mut State = &mut guard;
        for job in state.jobs.values_mut() {
            if job.state != JobState::Enabled {
                continue;
            }
            let Some(next) = job.next_run_at else {
                continue;
            };
            if next > now {
                continue;
            }
            let slots = collect_due_slots(job, now, max_catch_up);
            if slots.is_empty() {
                continue;
            }
            let fires: Vec<u64> = match (slots.len(), job.misfire) {
                (1, _) => slots,
                (_, MisfirePolicy::Skip) => Vec::new(),
                (_, MisfirePolicy::FireOnce) => vec![slots[slots.len() - 1]],
                (_, MisfirePolicy::CatchUp) => slots,
            };
            let fires: Vec<u64> = fires
                .into_iter()
                .filter(|slot| Some(*slot) != job.last_slot)
                .collect();
            if let Some(slot) = fires.last().copied() {
                job.last_slot = Some(slot);
                job.last_run_at = Some(now);
            }
            job.next_run_at = job.next_slot_after(now);
            for _ in &fires {
                spawns.push((
                    job.id.clone(),
                    job.queue.clone(),
                    job.name.clone(),
                    job.max_attempts,
                ));
            }
        }
    }
    let mut fires = 0_usize;
    for (job_id, queue, name, max_attempts) in spawns {
        let task_id = enqueue(
            shared,
            Task::new(queue)
                .name(name)
                .max_attempts(max_attempts)
                .tag(format!("job:{job_id}")),
        );
        if let Some(job) = lock(&shared.state).jobs.get_mut(&job_id) {
            job.last_task_id = Some(task_id);
        }
        shared.metrics.inc_jobs_fired();
        fires += 1;
    }
    fires
}

fn update_load(shared: &Arc<Shared>) {
    let load = {
        let state = lock(&shared.state);
        if state.inflight > 0 {
            Load::Busy
        } else if state.queue.is_empty()
            && state
                .jobs
                .values()
                .all(|job| job.state == JobState::Disabled)
        {
            Load::Idle
        } else {
            Load::Normal
        }
    };
    shared.heartbeat.set_load(load);
}

fn enqueue(shared: &Arc<Shared>, mut task: Task) -> TaskId {
    // 业务心跳：为未显式设置超时的任务注入看门狗默认超时（打标以便区分告警）
    if task.timeout.is_none() {
        if let Some(limit) = shared.watchdog {
            task.timeout = Some(limit);
            task.tags.push(WATCHDOG_TAG.to_string());
        }
    }
    let id = task.id.clone();
    let priority = task.priority;
    {
        let mut state = lock(&shared.state);
        state.seq += 1;
        let seq = state.seq;
        let replaced = state.tasks.insert(id.clone(), task);
        debug_assert!(replaced.is_none(), "duplicate task id");
        state
            .queue
            .push_ready(ReadyEntry::new(priority, seq, id.clone()));
    }
    shared.metrics.inc_submitted();
    shared.ready_cv.notify_one();
    emit(shared, &id, TaskState::Pending, None, None);
    id
}

fn collect_due_slots(job: &Job, now_ms: u64, max_catch_up: u32) -> Vec<u64> {
    let mut slots = Vec::new();
    let mut slot = job.next_run_at;
    let limit = max_catch_up.saturating_add(1) as usize;
    while let Some(current) = slot {
        if current > now_ms || slots.len() >= limit {
            break;
        }
        slots.push(current);
        slot = match job.next_slot_after(current) {
            Some(next) if next > current => Some(next),
            _ => None,
        };
    }
    slots
}

/// 投递一条错误事件（`event.error`；未绑定事件总线时为空操作）。
fn emit_error(shared: &Shared, error: &Error, reference: Option<&str>) {
    let bus = lock(&shared.events).clone();
    let Some(bus) = bus else {
        return;
    };
    bus.publish_error(
        shared.clock.now_ms(),
        error.code(),
        error.code_name(),
        &error.to_string(),
        reference,
    );
}

/// 投递一条任务事件（未绑定事件总线时为空操作）。
fn emit(
    shared: &Shared,
    task_id: &TaskId,
    state: TaskState,
    attempt: Option<u32>,
    error: Option<String>,
) {
    let bus = lock(&shared.events).clone();
    let Some(bus) = bus else {
        return;
    };
    let mut data = vec![
        ("taskId".to_string(), Value::String(task_id.to_string())),
        (
            "state".to_string(),
            Value::String(state.as_str().to_string()),
        ),
    ];
    if let Some(attempt) = attempt {
        data.push(("attempt".to_string(), Value::Number(f64::from(attempt))));
    }
    if let Some(error) = error {
        data.push(("err".to_string(), Value::String(error)));
    }
    bus.publish(shared.clock.now_ms(), "task", data);
}

fn set_state(task: &mut Task, next: TaskState) {
    if let Err(err) = task.transition(next) {
        log::warn(&format!("状态迁移失败: {err}"));
    }
}

fn backoff(shared: &Shared, config: &SchedulerConfig, attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1).min(63);
    let factor: u128 = 1_u128 << shift;
    let base = config.retry_base.as_millis();
    let cap = config.retry_max.as_millis();
    let delay = base.saturating_mul(factor).min(cap);
    let mut value = u64::try_from(delay).unwrap_or(u64::MAX);
    if config.jitter {
        // 加入最多 50% 的随机抖动，避免重试风暴
        let span = (value / 2).max(1);
        let mut rng = lock(&shared.rng);
        value = value.saturating_add(rng.next_below(span));
    }
    Duration::from_millis(value)
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
