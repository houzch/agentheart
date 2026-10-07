// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! AgentHeart Sidecar：以独立进程提供内核接口服务。
//!
//! 启动后打印监听地址与访问令牌（供宿主 / SDK 连接），随后常驻运行。
//! 关闭方式：终止进程（内核资源随进程退出释放）。
//!
//! 定时任务持久化到 `<AGENTHEART_DATA_DIR>/jobs.wal`（缺省 `./agentheart-data/jobs.wal`），
//! 重启时回放恢复；队列 / 循环 / 规则目前仍为内存态。

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agentheart_core::{
    Broker, BrokerConfig, EventBus, Handler, Kernel, MqttServer, Scheduler, SchedulerConfig,
    Server, SystemClock, Task, Wal,
};

fn main() {
    let handler: Handler = Arc::new(|_task: &Task| Ok(()));
    let wal_path = data_dir().join("jobs.wal");
    // 定时任务属低频配置类写入，逐条 fsync 换取更强持久性
    let wal = match Wal::open(wal_path.clone()) {
        Ok(wal) => wal.with_sync(true),
        Err(error) => {
            eprintln!(
                "agentheartd 打开 WAL 失败（{}）: {error}",
                wal_path.display()
            );
            std::process::exit(1);
        }
    };
    let scheduler = match Scheduler::start_with_journal(SchedulerConfig::default(), handler, wal) {
        Ok(scheduler) => Arc::new(scheduler),
        Err(error) => {
            eprintln!(
                "agentheartd WAL 回放失败（{}）: {error}",
                wal_path.display()
            );
            std::process::exit(1);
        }
    };
    let broker = Arc::new(Broker::new(BrokerConfig::default(), Arc::new(SystemClock)));
    let events = Arc::new(EventBus::new(4096));
    let kernel = Arc::new(Kernel::new(scheduler, broker, events));

    let token = generate_token();

    // MQTT 3.1.1 子集（topic 即队列名）：供外部 MQTT 客户端对接
    let mqtt = match MqttServer::bind(Arc::clone(&kernel.broker), "127.0.0.1:0", "") {
        Ok(server) => server,
        Err(error) => {
            eprintln!("agentheartd MQTT 启动失败: {error}");
            std::process::exit(1);
        }
    };
    mqtt.start();

    let server = match Server::bind(Arc::clone(&kernel), "127.0.0.1:0", token.clone()) {
        Ok(server) => server,
        Err(error) => {
            eprintln!("agentheartd 启动失败: {error}");
            std::process::exit(1);
        }
    };
    server.start();
    println!(
        "agentheartd ready addr={} token={token}",
        server.local_addr()
    );
    println!("agentheartd mqtt={} （topic 即队列名）", mqtt.local_addr());
    println!(
        "agentheartd wal={} （定时任务持久化；AGENTHEART_DATA_DIR 可改数据目录）",
        wal_path.display()
    );
    println!("agentheartd 协议：16 字节帧头 + JSON 载荷（详见方案第 8.3 节）");

    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}

/// 数据目录：环境变量 `AGENTHEART_DATA_DIR`，缺省为当前目录下的 `agentheart-data`。
fn data_dir() -> PathBuf {
    std::env::var_os("AGENTHEART_DATA_DIR")
        .map_or_else(|| PathBuf::from("agentheart-data"), PathBuf::from)
}

fn generate_token() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(1);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    let sequence = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:x}-{sequence:x}")
}
