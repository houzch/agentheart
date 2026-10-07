// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 心跳驱动器：以可配置间隔驱动 tick，支持空闲/繁忙自适应与即时唤醒。
//!
//! ## 自适应规则
//!
//! 仅在 `adaptive` 开启时生效；生效间隔按负载档位计算，并钳制在 `[min_interval, max_interval]`：
//!
//! | 负载 | 条件 | 目标间隔 |
//! | --- | --- | --- |
//! | 空闲 `Idle` | 无待执行/在途任务且无启用定时任务 | `base * idle_factor` |
//! | 繁忙 `Busy` | 有在途任务 | `base / busy_factor` |
//! | 普通 `Normal` | 其它 | `base` |

use std::sync::{Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

/// 负载档位（用于自适应心跳）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Load {
    /// 空闲：拉长间隔。
    Idle,
    /// 普通：基础间隔。
    Normal,
    /// 繁忙：缩短间隔。
    Busy,
}

/// 心跳配置。
#[derive(Debug, Clone, Copy)]
pub(crate) struct HeartbeatConfig {
    /// 基础间隔。
    pub(crate) interval: Duration,
    /// 是否启用自适应（空闲拉长 / 繁忙缩短）。
    pub(crate) adaptive: bool,
    /// 空闲时的拉长因子（至少为 1）。
    pub(crate) idle_factor: u32,
    /// 繁忙时的缩短因子（至少为 1）。
    pub(crate) busy_factor: u32,
    /// 自适应间隔下限。
    pub(crate) min_interval: Duration,
    /// 自适应间隔上限。
    pub(crate) max_interval: Duration,
}

#[derive(Debug)]
struct State {
    base: Duration,
    effective: Duration,
    adaptive: bool,
    idle_factor: u32,
    busy_factor: u32,
    min_interval: Duration,
    max_interval: Duration,
    lag_ms: u64,
    shutdown: bool,
}

/// 心跳驱动器。
#[derive(Debug)]
pub(crate) struct Heartbeat {
    state: Mutex<State>,
    cv: Condvar,
}

impl Heartbeat {
    /// 构造。
    pub(crate) fn new(config: HeartbeatConfig) -> Self {
        let min_interval = config.min_interval.max(Duration::from_millis(1));
        let max_interval = config.max_interval.max(min_interval);
        let base = clamp(config.interval, min_interval, max_interval);
        Self {
            state: Mutex::new(State {
                base,
                effective: base,
                adaptive: config.adaptive,
                idle_factor: config.idle_factor.max(1),
                busy_factor: config.busy_factor.max(1),
                min_interval,
                max_interval,
                lag_ms: 0,
                shutdown: false,
            }),
            cv: Condvar::new(),
        }
    }

    /// 等待下一次心跳；返回 `false` 表示已请求关闭。
    pub(crate) fn wait_next(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.shutdown {
            return false;
        }
        let interval = state.effective;
        let started = Instant::now();
        let (mut next, _) = self
            .cv
            .wait_timeout(state, interval)
            .unwrap_or_else(PoisonError::into_inner);
        if next.shutdown {
            return false;
        }
        // 实际等待超出目标间隔的部分即为滞后；被提前唤醒时为 0
        next.lag_ms = u64::try_from(started.elapsed().saturating_sub(interval).as_millis())
            .unwrap_or(u64::MAX);
        true
    }

    /// 唤醒等待中的心跳线程。
    pub(crate) fn wake(&self) {
        self.cv.notify_all();
    }

    /// 请求关闭。
    pub(crate) fn request_shutdown(&self) {
        {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.shutdown = true;
        }
        self.wake();
    }

    /// 根据负载档位调整生效间隔（仅自适应开启时生效）。
    pub(crate) fn set_load(&self, load: Load) {
        let changed = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if !state.adaptive {
                return;
            }
            let target = match load {
                Load::Idle => state.base.saturating_mul(state.idle_factor),
                Load::Busy => state.base / state.busy_factor,
                Load::Normal => state.base,
            };
            let target = clamp(target, state.min_interval, state.max_interval);
            if target == state.effective {
                false
            } else {
                state.effective = target;
                true
            }
        };
        if changed {
            self.wake();
        }
    }

    /// 当前生效间隔（毫秒）。
    pub(crate) fn interval_ms(&self) -> u64 {
        millis(
            self.state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .effective,
        )
    }

    /// 是否启用自适应。
    pub(crate) fn is_adaptive(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .adaptive
    }

    /// 最近一次心跳的滞后（毫秒）。
    pub(crate) fn lag_ms(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .lag_ms
    }
}

fn clamp(value: Duration, low: Duration, high: Duration) -> Duration {
    if value < low {
        low
    } else if value > high {
        high
    } else {
        value
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
