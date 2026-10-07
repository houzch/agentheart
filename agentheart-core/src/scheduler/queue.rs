//! 调度队列：优先级就绪队列 + 延时就绪（重试）队列。

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};

use crate::task::{Priority, TaskId};

/// 就绪队列条目：优先级高者先出；同优先级按入队序号 FIFO。
#[derive(Debug)]
pub(crate) struct ReadyEntry {
    priority: Priority,
    seq: u64,
    id: TaskId,
}

impl ReadyEntry {
    /// 构造条目。
    pub(crate) fn new(priority: Priority, seq: u64, id: TaskId) -> Self {
        Self { priority, seq, id }
    }

    /// 任务 ID。
    pub(crate) fn id(&self) -> &TaskId {
        &self.id
    }
}

impl PartialEq for ReadyEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for ReadyEntry {}

impl PartialOrd for ReadyEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReadyEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap 是最大堆：优先级大者先出；同优先级时序号小者先出（FIFO）。
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

/// 任务队列：就绪（优先级堆）+ 延时（按到期时间索引，单位为 Unix 毫秒）。
#[derive(Debug, Default)]
pub(crate) struct TaskQueue {
    ready: BinaryHeap<ReadyEntry>,
    delayed: BTreeMap<u64, Vec<TaskId>>,
}

impl TaskQueue {
    /// 推入就绪队列。
    pub(crate) fn push_ready(&mut self, entry: ReadyEntry) {
        self.ready.push(entry);
    }

    /// 弹出最高优先级的就绪条目。
    pub(crate) fn pop_ready(&mut self) -> Option<ReadyEntry> {
        self.ready.pop()
    }

    /// 加入延时队列（用于重试退避），`at_ms` 为到期 Unix 毫秒。
    pub(crate) fn push_delayed(&mut self, at_ms: u64, id: TaskId) {
        self.delayed.entry(at_ms).or_default().push(id);
    }

    /// 取出所有已到期的延时任务 ID。
    pub(crate) fn take_due(&mut self, now_ms: u64) -> Vec<TaskId> {
        let mut due = Vec::new();
        while let Some((&at, _)) = self.delayed.iter().next() {
            if at > now_ms {
                break;
            }
            if let Some((_, ids)) = self.delayed.pop_first() {
                due.extend(ids);
            }
        }
        due
    }

    /// 从就绪与延时队列中移除指定任务（用于暂停 / 重试后的重新入队）。
    pub(crate) fn remove(&mut self, id: &TaskId) {
        self.ready.retain(|entry| entry.id() != id);
        for ids in self.delayed.values_mut() {
            ids.retain(|item| item != id);
        }
        self.delayed.retain(|_, ids| !ids.is_empty());
    }

    /// 就绪队列长度。
    pub(crate) fn ready_len(&self) -> usize {
        self.ready.len()
    }

    /// 延时队列长度（按任务数计）。
    pub(crate) fn delayed_len(&self) -> usize {
        self.delayed.values().map(Vec::len).sum()
    }

    /// 就绪与延时队列是否均为空。
    pub(crate) fn is_empty(&self) -> bool {
        self.ready.is_empty() && self.delayed.is_empty()
    }
}
