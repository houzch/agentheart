//! 自动化规则引擎（M8）：事件 → 动作的事件驱动自动化。
//!
//! - [`RuleSpec`] / [`Rule`]：规则规格与记录（事件类型 + 字段过滤器 + 动作）；
//! - [`RuleAction`]：`trigger_job` / `publish` / `trigger_loop`；
//! - [`RuleEngine`]：随心跳轮询事件总线，命中规则即执行动作并投递 `event.rule`。
//!
//! 仅使用 std（零第三方依赖）。

mod engine;
mod model;

pub use engine::{EVENT_KINDS, RuleEngine};
pub use model::{Rule, RuleAction, RuleFilter, RuleId, RuleSpec};
