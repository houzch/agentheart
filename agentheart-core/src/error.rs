// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 内核统一错误类型。
//!
//! 错误码与方案第 8.3.8 节的内核接口协议保持一致。

use std::fmt;
use std::io;

use crate::task::TaskState;

/// 内核统一错误。
#[derive(Debug)]
pub enum Error {
    /// 目标不存在。
    NotFound {
        /// 资源类别（如 `"task"`）。
        kind: &'static str,
        /// 资源标识。
        id: String,
    },
    /// 非法状态迁移。
    InvalidTransition {
        /// 原状态。
        from: TaskState,
        /// 目标状态。
        to: TaskState,
    },
    /// 协议或参数错误。
    Protocol(&'static str),
    /// JSON 解析错误。
    Json {
        /// 错误详情。
        message: String,
    },
    /// 执行超时。
    Timeout {
        /// 任务标识。
        id: String,
    },
    /// 过载或超出在途上限。
    Busy,
    /// 资源冲突。
    Conflict {
        /// 资源标识。
        resource: String,
    },
    /// IO 错误。
    Io(io::Error),
    /// 内核内部错误。
    Internal(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound { kind, id } => write!(f, "{kind} not found: {id}"),
            Self::InvalidTransition { from, to } => {
                write!(f, "invalid transition: {from} -> {to}")
            }
            Self::Protocol(message) => write!(f, "protocol error: {message}"),
            Self::Json { message } => write!(f, "json error: {message}"),
            Self::Timeout { id } => write!(f, "task timed out: {id}"),
            Self::Busy => f.write_str("busy"),
            Self::Conflict { resource } => write!(f, "resource conflict: {resource}"),
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Internal(message) => write!(f, "internal error: {message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl Error {
    /// 协议错误码（见方案 8.3.8）。
    pub fn code(&self) -> i32 {
        match self {
            Self::NotFound { .. } => 5,
            Self::Protocol(_) | Self::InvalidTransition { .. } | Self::Json { .. } => 6,
            Self::Busy => 7,
            Self::Conflict { .. } => 8,
            Self::Timeout { .. } | Self::Io(_) | Self::Internal(_) => 10,
        }
    }

    /// 协议错误码名称。
    pub fn code_name(&self) -> &'static str {
        match self {
            Self::NotFound { .. } => "NOT_FOUND",
            Self::Protocol(_) | Self::InvalidTransition { .. } | Self::Json { .. } => "BAD_REQUEST",
            Self::Busy => "BUSY",
            Self::Conflict { .. } => "CONFLICT",
            Self::Timeout { .. } | Self::Io(_) | Self::Internal(_) => "INTERNAL",
        }
    }
}

/// 内核统一结果类型。
pub type Result<T> = std::result::Result<T, Error>;
