// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 时钟抽象：便于测试注入确定性时间。
//!
//! 内核内部统一通过 [`Clock`] 读取时间；生产环境使用 [`SystemClock`]，
//! 测试可注入 [`ManualClock`] 以获得确定性。

use std::sync::{Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

/// 时钟抽象（返回 Unix 毫秒时间戳）。
pub trait Clock: Send + Sync + 'static {
    /// 当前 Unix 毫秒时间戳。
    fn now_ms(&self) -> u64;
}

/// 系统时钟（默认）。
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
            })
    }
}

/// 手动时钟（测试用）：可显式推进或设置时间。
#[derive(Debug)]
pub struct ManualClock {
    now: Mutex<u64>,
}

impl ManualClock {
    /// 以给定起点创建。
    pub fn new(start_ms: u64) -> Self {
        Self {
            now: Mutex::new(start_ms),
        }
    }

    /// 前进 `delta_ms` 毫秒。
    pub fn advance(&self, delta_ms: u64) {
        let mut now = self.now.lock().unwrap_or_else(PoisonError::into_inner);
        *now = now.saturating_add(delta_ms);
    }

    /// 设置当前时间。
    pub fn set(&self, now_ms: u64) {
        let mut now = self.now.lock().unwrap_or_else(PoisonError::into_inner);
        *now = now_ms;
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        *self.now.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
