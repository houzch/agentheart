// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 「并发冲突治理」示例（M11）：同资源串行 + 幂等去重。
//!
//! 演示阶段一 M4 的关键能力：同一**资源键**（`Task::key`）的任务串行执行，
//! 不会并发互相踩踏；相同**幂等键**（`Task::idempotency_key`）的重复提交只入队一次。
//!
//! 运行：`cargo run -p agentheart-adapters --example concurrency_guard`
//! 成功时打印 `CONCURRENCY_GUARD_OK` 并以 0 退出。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use agentheart_core::{Handler, Scheduler, SchedulerConfig, Task};

/// 每个任务持有的时间。
const HOLD: Duration = Duration::from_millis(30);
/// 同资源任务数。
const SAME_KEY_TASKS: usize = 4;
/// 整体超时。
const DEADLINE: Duration = Duration::from_secs(10);

fn main() {
    match run() {
        Ok(()) => println!("CONCURRENCY_GUARD_OK"),
        Err(message) => {
            eprintln!("CONCURRENCY_GUARD_FAILED: {message}");
            std::process::exit(1);
        }
    }
}

fn run() -> Result<(), String> {
    // 仅统计**带资源键**的任务并发度（无键任务本就可并行，不参与判定）
    let keyed_running = Arc::new(AtomicUsize::new(0));
    let keyed_max = Arc::new(AtomicUsize::new(0));
    let keyed_done = Arc::new(AtomicUsize::new(0));

    let handler: Handler = {
        let running = Arc::clone(&keyed_running);
        let max_seen = Arc::clone(&keyed_max);
        let done = Arc::clone(&keyed_done);
        Arc::new(move |task: &Task| {
            if task.key.is_some() {
                let current = running.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(current, Ordering::SeqCst);
                std::thread::sleep(HOLD);
                running.fetch_sub(1, Ordering::SeqCst);
                done.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        })
    };

    // 2 个工作线程：若资源键未生效，同资源任务会并发（max_seen > 1）
    let config = SchedulerConfig {
        workers: 2,
        heartbeat_interval: Duration::from_millis(25),
        ..SchedulerConfig::default()
    };
    let scheduler = Scheduler::start_with(config, handler);

    // 1) 同资源键 → 串行
    let mut ids = Vec::new();
    for index in 0..SAME_KEY_TASKS {
        ids.push(
            scheduler
                .submit(
                    Task::new("shared-resource")
                        .name(format!("job-{index}"))
                        .key("resource:x"),
                )
                .map_err(|error| error.to_string())?,
        );
    }

    // 2) 幂等键 → 重复提交只入队一次
    let first = scheduler
        .submit(Task::new("idempotent").idempotency_key("once-only"))
        .map_err(|error| error.to_string())?;
    let second = scheduler
        .submit(Task::new("idempotent").idempotency_key("once-only"))
        .map_err(|error| error.to_string())?;

    // 等待同资源任务全部完成
    let started = Instant::now();
    loop {
        if keyed_done.load(Ordering::SeqCst) >= SAME_KEY_TASKS {
            break;
        }
        if started.elapsed() >= DEADLINE {
            scheduler.shutdown();
            return Err(format!(
                "超时未完成：keyed_done={}",
                keyed_done.load(Ordering::SeqCst)
            ));
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    let observed = keyed_max.load(Ordering::SeqCst);
    let deduped = first == second;
    println!("—— 并发治理结果 ——");
    println!("  已提交同资源任务：{} 个（资源键 resource:x）", ids.len());
    println!("  观测到的最大并发度：{observed}（期望 1，串行）");
    println!("  幂等去重：{first} == {second} → {deduped}");
    scheduler.shutdown();

    if observed != 1 {
        return Err(format!("同资源任务出现并发：max={observed}"));
    }
    if !deduped {
        return Err("幂等键未生效：重复提交生成了不同任务".to_string());
    }
    Ok(())
}
