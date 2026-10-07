// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 调度中心：心跳驱动、优先级队列、线程池执行、超时与重试。

mod core;
mod heartbeat;
mod queue;

pub use core::{Handler, PauseScope, Scheduler, SchedulerConfig, SchedulerStats, TickHook};
