//! 自研伪随机数（零依赖）：用于重试退避抖动。
//!
//! 采用 xorshift64* 变体，以系统时间与进程号播种，满足"抖动"用途，非密码学安全。

use std::time::{SystemTime, UNIX_EPOCH};

/// 伪随机数生成器。
#[derive(Debug)]
pub(crate) struct Rng {
    state: u64,
}

impl Rng {
    /// 以系统时间与进程号播种。
    pub(crate) fn from_entropy() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
            });
        let seed = nanos ^ (u64::from(std::process::id()) << 32) ^ 0x9E37_79B9_7F4A_7C15;
        Self {
            state: if seed == 0 {
                0x1234_5678_9ABC_DEF0
            } else {
                seed
            },
        }
    }

    /// 下一个 64 位随机数。
    pub(crate) fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// 返回 `[0, bound)` 内的随机数；`bound` 为 0 时返回 0。
    pub(crate) fn next_below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            0
        } else {
            self.next_u64() % bound
        }
    }
}
