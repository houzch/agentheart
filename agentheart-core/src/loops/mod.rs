// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 循环任务引擎（M7）：把「反复执行直至收敛/达上限」下沉为内核原语。
//!
//! - [`LoopSpec`]：循环规格（迭代上限、间隔、退避、超时、截止、失败策略）；
//! - [`Loop`] / [`LoopState`]：循环记录与状态机；
//! - [`LoopController`]：随心跳推进迭代、把迭代包装为任务并串行执行。
//!
//! 仅使用 std（零第三方依赖）。

mod controller;
mod model;

pub use controller::{LoopConfig, LoopController};
pub use model::{Loop, LoopId, LoopOnError, LoopSpec, LoopState};
