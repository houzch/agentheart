// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 幂等去重表（带上限的 FIFO 淘汰）。

use std::collections::{HashMap, VecDeque};

use crate::task::TaskId;

/// 幂等键 -> 任务 ID 的映射；超出容量时淘汰最早的键。
#[derive(Debug)]
pub(crate) struct IdempotencyMap {
    entries: HashMap<String, TaskId>,
    order: VecDeque<String>,
    capacity: usize,
}

impl IdempotencyMap {
    /// 以容量上限创建。
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    /// 查询键对应的任务 ID。
    pub(crate) fn get(&self, key: &str) -> Option<&TaskId> {
        self.entries.get(key)
    }

    /// 记录键与任务 ID（已存在则不覆盖）。
    pub(crate) fn insert(&mut self, key: String, id: TaskId) {
        if self.entries.contains_key(&key) {
            return;
        }
        self.entries.insert(key.clone(), id);
        self.order.push_back(key);
        while self.order.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
    }
}
