// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 指标计数（零依赖，基于 `std::sync::atomic`）。
//!
//! 调用方通过 `inc_*` 方法累加，`snapshot` 生成一次性快照用于读取与对外暴露。

use std::sync::atomic::{AtomicU64, Ordering};

/// 内核运行指标（原子计数，可被多线程共享累加）。
#[derive(Debug, Default)]
pub struct Metrics {
    submitted: AtomicU64,
    started: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    retried: AtomicU64,
    dead: AtomicU64,
    timed_out: AtomicU64,
    jobs_fired: AtomicU64,
    throttled: AtomicU64,
}

impl Metrics {
    /// 累计提交数 +1。
    pub fn inc_submitted(&self) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
    }

    /// 累计开始执行数 +1。
    pub fn inc_started(&self) {
        self.started.fetch_add(1, Ordering::Relaxed);
    }

    /// 累计成功数 +1。
    pub fn inc_succeeded(&self) {
        self.succeeded.fetch_add(1, Ordering::Relaxed);
    }

    /// 累计失败数 +1。
    pub fn inc_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    /// 累计重试数 +1。
    pub fn inc_retried(&self) {
        self.retried.fetch_add(1, Ordering::Relaxed);
    }

    /// 累计死信数 +1。
    pub fn inc_dead(&self) {
        self.dead.fetch_add(1, Ordering::Relaxed);
    }

    /// 累计超时数 +1。
    pub fn inc_timed_out(&self) {
        self.timed_out.fetch_add(1, Ordering::Relaxed);
    }

    /// 累计定时任务触发数 +1。
    pub fn inc_jobs_fired(&self) {
        self.jobs_fired.fetch_add(1, Ordering::Relaxed);
    }

    /// 累计被限流（令牌不足）数 +1。
    pub fn inc_throttled(&self) {
        self.throttled.fetch_add(1, Ordering::Relaxed);
    }

    /// 生成一次性快照。
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            submitted: self.submitted.load(Ordering::Relaxed),
            started: self.started.load(Ordering::Relaxed),
            succeeded: self.succeeded.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            retried: self.retried.load(Ordering::Relaxed),
            dead: self.dead.load(Ordering::Relaxed),
            timed_out: self.timed_out.load(Ordering::Relaxed),
            jobs_fired: self.jobs_fired.load(Ordering::Relaxed),
            throttled: self.throttled.load(Ordering::Relaxed),
        }
    }
}

/// 指标快照（不可变）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    /// 累计提交数。
    pub submitted: u64,
    /// 累计开始执行数。
    pub started: u64,
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
    pub jobs_fired: u64,
    /// 累计被限流数。
    pub throttled: u64,
}
