//! 最小可用自研 JSON 编解码（零第三方依赖）。

mod codec;

pub use codec::{Value, parse, parse_bytes};
