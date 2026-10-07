//! C ABI 导出（供 `cdylib` 宿主以标准 FFI 调用）。
//!
//! 约定：
//! - 导出符号统一 `ah_` 前缀；
//! - 返回 `i32` 错误码，`0` 表示成功（错误码见方案 8.3.8）；
//! - 字符串统一为 **NUL 结尾的 UTF-8**；
//! - 边界处使用 `catch_unwind`，**panic 不跨越 FFI**。

use std::ffi::{CStr, CString, c_char, c_int};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::clock::SystemClock;
use crate::json::{self, Value};
use crate::protocol::{EventBus, Kernel, PROTOCOL_VERSION};
use crate::queue::{Broker, BrokerConfig};
use crate::scheduler::{Handler, Scheduler, SchedulerConfig};
use crate::task::Task;

/// 事件订阅状态（内嵌承载）。
#[derive(Debug, Default)]
struct Subscription {
    topics: Vec<String>,
    cursor: u64,
}

/// 不透明的内核句柄。
#[derive(Debug)]
pub struct AhHandle {
    kernel: Arc<Kernel>,
    subscription: Mutex<Subscription>,
}

/// 返回协议版本。
#[unsafe(no_mangle)]
pub extern "C" fn ah_version() -> c_int {
    i32::from(PROTOCOL_VERSION)
}

/// 创建内核（默认配置），成功时把句柄写入 `out`。
///
/// 返回 `0` 成功；`1` 参数非法；`10` 内部错误。
///
/// # Safety
/// `out` 必须是可写的有效指针。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ah_open(out: *mut *mut AhHandle) -> c_int {
    if out.is_null() {
        return 1;
    }
    let created = catch_unwind(AssertUnwindSafe(|| {
        let handler: Handler = Arc::new(|_task: &Task| Ok(()));
        let scheduler = Arc::new(Scheduler::start_with(SchedulerConfig::default(), handler));
        let broker = Arc::new(Broker::new(BrokerConfig::default(), Arc::new(SystemClock)));
        let events = Arc::new(EventBus::new(1024));
        let kernel = Arc::new(Kernel::new(scheduler, broker, events));
        Box::into_raw(Box::new(AhHandle {
            kernel,
            subscription: Mutex::new(Subscription::default()),
        }))
    }));
    match created {
        Ok(handle) => {
            // SAFETY: 调用方保证 out 有效
            unsafe { *out = handle };
            0
        }
        Err(_) => 10,
    }
}

/// 处理一次请求：`request` 为请求 JSON，结果以新分配的 C 字符串写入 `out`。
///
/// 返回 `0` 成功；`1` 参数非法；`6` 请求非法；`10` 内部错误。
///
/// # Safety
/// `handle` 必须来自 [`ah_open`]；`request` 须为 NUL 结尾的 UTF-8；`out` 必须可写。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ah_call(
    handle: *mut AhHandle,
    request: *const c_char,
    out: *mut *mut c_char,
) -> c_int {
    if handle.is_null() || request.is_null() || out.is_null() {
        return 1;
    }
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: 由调用约定保证指针有效
        let kernel = unsafe { &(*handle).kernel };
        // SAFETY: request 为 NUL 结尾字符串
        let text = unsafe { CStr::from_ptr(request) }.to_str().map_err(|_| 6)?;
        let parsed = json::parse(text).map_err(|_| 6)?;
        let response = kernel.handle(&parsed).to_json_string();
        CString::new(response).map_err(|_| 10)
    }));
    match outcome {
        Ok(Ok(cstring)) => {
            // SAFETY: out 有效
            unsafe { *out = cstring.into_raw() };
            0
        }
        Ok(Err(code)) => code,
        Err(_) => 10,
    }
}

/// 释放 [`ah_call`] 返回的字符串。
///
/// # Safety
/// `ptr` 必须来自 [`ah_call`]，且只能释放一次。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ah_string_free(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: 由 CString::into_raw 分配
    unsafe { drop(CString::from_raw(ptr)) };
}

/// 订阅事件流：`topics` 为 JSON 数组字符串（如 `["task"]`；`[]` 或 `["*"]` 表示全部）。
///
/// 结果以新分配的 C 字符串写入 `out`（须用 [`ah_string_free`] 释放）。
/// 返回 `0` 成功；`1` 参数非法；`6` 请求非法；`10` 内部错误。
///
/// # Safety
/// `handle` 必须来自 [`ah_open`]；`topics` 须为 NUL 结尾的 UTF-8 JSON 数组；`out` 必须可写。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ah_subscribe(
    handle: *mut AhHandle,
    topics: *const c_char,
    from_seq: u64,
    out: *mut *mut c_char,
) -> c_int {
    if handle.is_null() || topics.is_null() || out.is_null() {
        return 1;
    }
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: 由调用约定保证指针有效
        let handle = unsafe { &*handle };
        // SAFETY: topics 为 NUL 结尾字符串
        let text = unsafe { CStr::from_ptr(topics) }.to_str().map_err(|_| 6)?;
        let parsed = json::parse(text).map_err(|_| 6)?;
        let list = parsed.as_array().ok_or(6)?;
        let mut wanted = Vec::with_capacity(list.len());
        for item in list {
            wanted.push(item.as_str().ok_or(6)?.to_string());
        }
        let seq = {
            let mut subscription = lock(&handle.subscription);
            subscription.topics = wanted;
            subscription.cursor = from_seq;
            handle.kernel.events.latest_seq()
        };
        let mut result = Value::object();
        result.insert("streamId", Value::String("ffi".to_string()));
        result.insert("seq", Value::Number(seq as f64));
        let mut response = Value::object();
        response.insert("m", Value::String("stream.subscribed".to_string()));
        response.insert("ok", Value::Bool(true));
        response.insert("result", result);
        CString::new(response.to_json_string()).map_err(|_| 10)
    }));
    match outcome {
        Ok(Ok(cstring)) => {
            // SAFETY: out 有效
            unsafe { *out = cstring.into_raw() };
            0
        }
        Ok(Err(code)) => code,
        Err(_) => 10,
    }
}

/// 拉取自上次拉取以来收到的事件（JSON 数组字符串；新分配，须用 [`ah_string_free`] 释放）。
///
/// 返回 `0` 成功；`1` 参数非法；`10` 内部错误。
///
/// # Safety
/// `handle` 必须来自 [`ah_open`]；`out` 必须可写。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ah_poll(handle: *mut AhHandle, out: *mut *mut c_char) -> c_int {
    if handle.is_null() || out.is_null() {
        return 1;
    }
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: 由调用约定保证指针有效
        let handle = unsafe { &*handle };
        let mut subscription = lock(&handle.subscription);
        let mut parts = Vec::new();
        for event in handle.kernel.events.poll_after(subscription.cursor) {
            subscription.cursor = event.seq;
            let matched = subscription.topics.is_empty()
                || subscription
                    .topics
                    .iter()
                    .any(|topic| topic == "*" || topic == &event.kind);
            if matched {
                parts.push(event.to_value().to_json_string());
            }
        }
        CString::new(format!("[{}]", parts.join(","))).map_err(|_| 10)
    }));
    match outcome {
        Ok(Ok(cstring)) => {
            // SAFETY: out 有效
            unsafe { *out = cstring.into_raw() };
            0
        }
        Ok(Err(code)) => code,
        Err(_) => 10,
    }
}

/// 关闭并释放内核句柄。
///
/// # Safety
/// `handle` 必须来自 [`ah_open`]，且只能关闭一次。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ah_close(handle: *mut AhHandle) {
    if handle.is_null() {
        return;
    }
    // SAFETY: 由 ah_open 通过 Box::into_raw 分配
    let owned = unsafe { Box::from_raw(handle) };
    owned.kernel.scheduler.shutdown();
    owned.kernel.broker.shutdown();
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// 便于测试：把 JSON 字符串转为内核可读的响应（内部使用）。
///
/// # Errors
/// JSON 非法时返回错误。
pub fn call_json(kernel: &Kernel, request: &str) -> crate::error::Result<Value> {
    let parsed = json::parse(request)?;
    Ok(kernel.handle(&parsed))
}
