//! AgentHeart 控制台（Tauri 2）后端。
//!
//! **内核外置**：本产物不属于内核，其依赖（Tauri 2）不进入内核依赖树
//! （`ui/src-tauri` 为独立工作区，见第 10.2 节）。
//!
//! 后端把全部内核交互委托给 [`agentheart_sdk::Session`]，仅做薄封装：
//!
//! - **双承载**：`socket`（连接 `agentheartd` 侧车）/ `embedded`（进程内直调内核）；
//! - **统一命令**：`connect` / `disconnect` / `call` / `poll_events` / `status`；
//! - **事件流**：连接后自动订阅全部主题，前端定期 `poll_events` 拉取增量。

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
#![warn(clippy::all)]

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use agentheart_core::json::Value;
use agentheart_core::{
    Broker, BrokerConfig, EventBus, Handler, Kernel, Scheduler, SchedulerConfig, SystemClock, Task,
};
use agentheart_sdk::Session;
use tauri::State;

/// 单次请求超时。
const CALL_TIMEOUT: Duration = Duration::from_secs(5);

/// 内嵌内核句柄（持有调度器与内核，保证其存活）。
struct EmbeddedKernel {
    #[allow(dead_code)]
    scheduler: Arc<Scheduler>,
    kernel: Arc<Kernel>,
}

/// 应用状态：当前会话（可选）与内嵌内核（可选）。
#[derive(Default)]
struct AppState {
    session: Mutex<Option<Session>>,
    embedded: Mutex<Option<EmbeddedKernel>>,
}

impl AppState {
    fn session(&self) -> MutexGuard<'_, Option<Session>> {
        self.session.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// 建立内嵌内核（无操作处理函数；控制台用于观察与控制调度中枢）。
fn build_embedded() -> EmbeddedKernel {
    let handler: Handler = Arc::new(|_task: &Task| Ok(()));
    let scheduler = Arc::new(Scheduler::start_with(SchedulerConfig::default(), handler));
    let broker = Arc::new(Broker::new(BrokerConfig::default(), Arc::new(SystemClock)));
    let events = Arc::new(EventBus::new(512));
    let kernel = Arc::new(Kernel::new(Arc::clone(&scheduler), broker, events));
    EmbeddedKernel { scheduler, kernel }
}

/// 连接内核：`mode` 为 `socket`（需 `addr`/`token`）或 `embedded`（忽略 `addr`/`token`）。
#[tauri::command]
fn connect(
    state: State<'_, AppState>,
    mode: String,
    addr: String,
    token: String,
) -> Result<String, String> {
    let mut session = match mode.as_str() {
        "embedded" => {
            let embedded = build_embedded();
            let handle = Session::embedded(Arc::clone(&embedded.kernel));
            *state
                .embedded
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(embedded);
            handle
        }
        _ => {
            let address = addr.parse().map_err(|error| format!("地址非法: {error}"))?;
            Session::connect(address, &token, CALL_TIMEOUT)
                .map_err(|error| format!("连接失败: {error}"))?
        }
    };

    // 自动订阅全部事件主题，供实时刷新
    session
        .subscribe(&["*"], 0)
        .map_err(|error| format!("订阅事件失败: {error}"))?;

    let mode = session.mode().to_string();
    *state.session() = Some(session);
    Ok(format!("{{\"mode\":\"{mode}\"}}"))
}

/// 断开当前会话（幂等）。
#[tauri::command]
fn disconnect(state: State<'_, AppState>) {
    *state.session() = None;
    *state
        .embedded
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = None;
}

/// 发送一次内核请求，返回响应 JSON。
#[tauri::command]
fn call(state: State<'_, AppState>, request: String) -> Result<String, String> {
    let mut guard = state.session();
    let session = guard.as_mut().ok_or("尚未连接内核")?;
    let value = session.call(&request).map_err(|error| error.to_string())?;
    Ok(value.to_json_string())
}

/// 拉取自上次拉取以来收到的事件（JSON 数组）。
#[tauri::command]
fn poll_events(state: State<'_, AppState>) -> Result<String, String> {
    let mut guard = state.session();
    let Some(session) = guard.as_mut() else {
        return Ok("[]".to_string());
    };
    let body = session
        .drain_events()
        .iter()
        .map(Value::to_json_string)
        .collect::<Vec<_>>()
        .join(",");
    Ok(format!("[{body}]"))
}

/// 连接状态：`{ connected, mode }`。
#[tauri::command]
fn status(state: State<'_, AppState>) -> String {
    let guard = state.session();
    let (connected, mode) = match guard.as_ref() {
        Some(session) => (true, session.mode()),
        None => (false, ""),
    };
    format!("{{\"connected\":{connected},\"mode\":\"{mode}\"}}")
}

fn main() {
    tauri::Builder::default()
        .manage(AppState::default())
        .invoke_handler(tauri::generate_handler![
            connect,
            disconnect,
            call,
            poll_events,
            status
        ])
        .run(tauri::generate_context!())
        .expect("运行 AgentHeart 控制台失败");
}
