//! 可观测：结构化日志、指标与事件流。
//!
//! 内核唯一的日志出口（`log`）、指标计数（`metrics`）与事件总线（`events`）。

mod events;
pub mod log;
pub mod metrics;

pub use events::{Event, EventBus};
pub use log::Level;
pub use metrics::{Metrics, MetricsSnapshot};
