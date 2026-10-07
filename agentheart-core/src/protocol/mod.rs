// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 内核接口协议：帧编解码、消息分发与本地 Socket 服务。
//!
//! 事件流（[`Event`] / [`EventBus`]）位于 `observe` 层，本模块对外重导出。

mod frame;
mod kernel;
pub mod mqtt;
mod pagination;
mod server;

pub use crate::observe::{Event, EventBus};
pub use frame::{
    Frame, HEADER_LEN, MAGIC, MAX_PAYLOAD, PROTOCOL_VERSION, flags, read_frame, write_frame,
};
pub use kernel::Kernel;
pub use mqtt::MqttServer;
pub use server::Server;
