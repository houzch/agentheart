// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 极简结构化日志（零依赖）。
//!
//! 本模块是内核**唯一**的日志出口：其它代码禁止直接使用 `println!` / `eprintln!`。
//! 输出写入 `stderr`，避免污染宿主进程的 `stdout`。

use std::sync::atomic::{AtomicU8, Ordering};

/// 日志级别（数值越大越详细）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Level {
    /// 错误。
    Error = 0,
    /// 警告。
    Warn = 1,
    /// 信息。
    Info = 2,
    /// 调试。
    Debug = 3,
    /// 追踪。
    Trace = 4,
}

impl Level {
    /// 级别名称。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Error => "ERROR",
            Self::Warn => "WARN",
            Self::Info => "INFO",
            Self::Debug => "DEBUG",
            Self::Trace => "TRACE",
        }
    }
}

/// 默认级别：`Info`。
static LEVEL: AtomicU8 = AtomicU8::new(Level::Info as u8);

/// 设置全局日志级别。
pub fn set_level(level: Level) {
    LEVEL.store(level as u8, Ordering::Relaxed);
}

/// 当前全局日志级别。
pub fn level() -> Level {
    match LEVEL.load(Ordering::Relaxed) {
        0 => Level::Error,
        1 => Level::Warn,
        2 => Level::Info,
        3 => Level::Debug,
        _ => Level::Trace,
    }
}

/// 记录一条日志（低于当前级别则忽略）。
pub fn log(at: Level, message: &str) {
    if at > level() {
        return;
    }
    eprintln!("[agentheart] {:<5} {message}", at.as_str());
}

/// 记录 `ERROR` 级日志。
pub fn error(message: &str) {
    log(Level::Error, message);
}

/// 记录 `WARN` 级日志。
pub fn warn(message: &str) {
    log(Level::Warn, message);
}

/// 记录 `INFO` 级日志。
pub fn info(message: &str) {
    log(Level::Info, message);
}

/// 记录 `DEBUG` 级日志。
pub fn debug(message: &str) {
    log(Level::Debug, message);
}
