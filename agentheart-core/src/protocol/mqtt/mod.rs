//! MQTT 3.1.1 子集（固定头 + 变长长度 + PUBLISH/SUBSCRIBE/PUBACK 等）。
//!
//! topic 与内核队列名一一对应，供外部 MQTT 客户端对接。

mod bridge;
mod packet;

pub use bridge::MqttServer;
