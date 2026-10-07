//! 令牌桶限流。

/// 令牌桶限流配置（公开，用于 [`crate::SchedulerConfig`]）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimit {
    /// 桶容量（突发上限）。
    pub capacity: u32,
    /// 每秒补充的令牌数。
    pub refill_per_sec: u32,
}

impl RateLimit {
    /// 构造。
    pub const fn new(capacity: u32, refill_per_sec: u32) -> Self {
        Self {
            capacity,
            refill_per_sec,
        }
    }
}

/// 令牌桶（内部状态）。
#[derive(Debug)]
pub(crate) struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_per_ms: f64,
    last_ms: u64,
}

impl TokenBucket {
    /// 由配置创建（容量至少为 1）。
    pub(crate) fn new(limit: RateLimit) -> Self {
        let capacity = f64::from(limit.capacity.max(1));
        Self {
            capacity,
            tokens: capacity,
            refill_per_ms: f64::from(limit.refill_per_sec) / 1000.0,
            last_ms: 0,
        }
    }

    /// 尝试取走一个令牌。
    pub(crate) fn try_acquire(&mut self, now_ms: u64) -> bool {
        if self.last_ms != 0 {
            let elapsed = now_ms.saturating_sub(self.last_ms) as f64;
            self.tokens = (self.tokens + elapsed * self.refill_per_ms).min(self.capacity);
        }
        self.last_ms = now_ms;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}
