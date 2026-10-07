// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 资源级互斥锁：同一资源键（如会话 ID）的任务串行执行。

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

/// 资源锁注册表。
#[derive(Debug, Default)]
pub(crate) struct ResourceLocks {
    map: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl ResourceLocks {
    /// 获取（或创建）资源键对应的互斥锁。
    pub(crate) fn lock_for(&self, key: &str) -> Arc<Mutex<()>> {
        let mut map = self.map.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(
            map.entry(key.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    }
}
