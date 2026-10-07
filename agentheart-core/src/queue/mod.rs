// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 消息队列：有界缓冲、背压、至少一次投递与死信。

mod broker;
mod message;

pub use broker::{Broker, BrokerConfig};
pub use message::{Message, MessageId, QueueStat};
