// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 并发治理：幂等去重、资源锁、限流、抖动退避与文件锁选主。
//!
//! - 幂等 / 资源锁 / 随机数 / 令牌桶均为内核内部实现；
//! - [`FileLock`] 与 [`RateLimit`] 对外公开（供宿主与 Sidecar 使用）。

mod file_lock;
pub(crate) mod idempotency;
pub(crate) mod random;
pub(crate) mod rate_limit;
pub(crate) mod resource_lock;

pub use file_lock::FileLock;
pub use rate_limit::RateLimit;
