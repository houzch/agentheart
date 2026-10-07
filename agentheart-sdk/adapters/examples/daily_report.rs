// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 「每日报告」端到端示例（M11）。
//!
//! 串起阶段二的能力：**cron 定时触发** → **命令适配器执行采集动作** →
//! **Loop 迭代收敛** → **自动化规则**在循环完成时把汇总投递到队列。
//!
//! 运行：`cargo run -p agentheart-adapters --example daily_report`
//! 成功时打印 `DAILY_REPORT_OK` 并以 0 退出。

use std::sync::Arc;
use std::time::{Duration, Instant};

use agentheart_adapters::CommandAdapter;
use agentheart_core::{
    Broker, BrokerConfig, EventBus, Handler, Job, JobId, Kernel, LoopController, LoopSpec,
    LoopState, RuleAction, RuleEngine, RuleFilter, RuleSpec, Scheduler, SchedulerConfig,
    SystemClock,
};

/// 采集队列 / 报告队列。
const COLLECT_QUEUE: &str = "collect";
const REPORT_QUEUE: &str = "report";
/// Loop 迭代次数。
const ITERATIONS: u64 = 3;
/// 整体超时。
const DEADLINE: Duration = Duration::from_secs(10);

fn main() {
    match run() {
        Ok(()) => println!("DAILY_REPORT_OK"),
        Err(message) => {
            eprintln!("DAILY_REPORT_FAILED: {message}");
            std::process::exit(1);
        }
    }
}

fn run() -> Result<(), String> {
    // 1) 调度器：短心跳，便于观察节拍
    let handler: Handler = {
        // 采集动作 = 命令适配器（`exit 0` 代表一次成功的采集）
        let adapter: agentheart_adapters::DynAdapter =
            Arc::new(CommandAdapter::from_shell("collect", "exit 0"));
        agentheart_adapters::handler(adapter)
    };
    let config = SchedulerConfig {
        workers: 2,
        heartbeat_interval: Duration::from_millis(25),
        ..SchedulerConfig::default()
    };
    let scheduler = Arc::new(Scheduler::start_with(config, handler));
    let broker = Arc::new(Broker::new(BrokerConfig::default(), Arc::new(SystemClock)));
    let events = Arc::new(EventBus::new(1024));

    // 2) 循环引擎 + 自动化规则引擎
    let loops = Arc::new(LoopController::new(Arc::clone(&scheduler)));
    let rules = Arc::new(RuleEngine::new(Arc::clone(&scheduler), Arc::clone(&broker)));
    let kernel = Kernel::new(
        Arc::clone(&scheduler),
        Arc::clone(&broker),
        Arc::clone(&events),
    )
    .with_loops(Arc::clone(&loops))
    .with_rules(Arc::clone(&rules));
    // 保持内核上下文存活（事件总线绑定、心跳钩子随 scheduler 运行）
    let _kernel = kernel;

    // 3) 每日 09:00 的定时任务（示例中手动触发一次以模拟到点）
    let job: JobId = scheduler
        .add_job(
            Job::from_cron("daily-report", COLLECT_QUEUE, "0 9 * * *")
                .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
    println!("[1/4] 已注册每日定时任务 job={job}（cron: 0 9 * * *）");

    // 4) 采集循环：最多 N 次迭代，完成后收敛
    let loop_id = loops
        .create(
            LoopSpec::new(COLLECT_QUEUE)
                .name("daily-report")
                .max_iterations(ITERATIONS)
                .interval_ms(0),
        )
        .map_err(|error| error.to_string())?;
    println!("[2/4] 已创建采集循环 loop={loop_id}（maxIterations={ITERATIONS}）");

    // 5) 规则：循环完成 → 汇总入报告队列
    rules
        .create(
            RuleSpec::new(
                "loop",
                RuleAction::Publish {
                    queue: REPORT_QUEUE.to_string(),
                    body: "daily-report-ready".to_string(),
                },
            )
            .name("loop-completed-to-report")
            .filter(RuleFilter {
                matches: vec![("phase".to_string(), "completed".to_string())],
            }),
        )
        .map_err(|error| error.to_string())?;
    println!("[3/4] 已创建规则：event.loop(completed) → queue:{REPORT_QUEUE}");

    // 6) 触发每日节拍（模拟 09:00 到点）
    let task_id = scheduler
        .trigger_job(&job)
        .map_err(|error| error.to_string())?;
    println!("[4/4] 手动触发每日任务（模拟 09:00）：task={task_id}");

    // 7) 等待：循环收敛 + 报告入队
    let started = Instant::now();
    loop {
        let loop_state = loops.get(&loop_id).map(|item| item.state);
        let report_depth = broker.stats(REPORT_QUEUE).map_or(0, |stat| stat.depth);
        if loop_state == Some(LoopState::Completed) && report_depth >= 1 {
            let iterations = loops.get(&loop_id).map_or(0, |item| item.iteration);
            let fired = scheduler.stats().job_fires;
            println!("—— 汇总 ——");
            println!("  循环迭代：{iterations}/{ITERATIONS}（状态 completed）");
            println!("  定时任务自动触发次数：{fired}（本次为手动触发模拟到点）");
            println!("  报告队列 {REPORT_QUEUE} 深度：{report_depth}");
            println!("  事件序号（最新）：{}", events.latest_seq());
            scheduler.shutdown();
            return Ok(());
        }
        if started.elapsed() >= DEADLINE {
            scheduler.shutdown();
            return Err(format!(
                "超时未达成：loop={:?} report_depth={report_depth}",
                loop_state
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
