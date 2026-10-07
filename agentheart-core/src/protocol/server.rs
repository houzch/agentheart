//! 内核接口服务：本地回环 Socket 服务端。
//!
//! 每个连接由「读线程 + 写线程」协作：读线程处理请求-响应，写线程负责事件推送。

use std::collections::HashSet;
use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::error::Result;
use crate::json::{self, Value};
use crate::observe::EventBus;

use super::frame::{self, Frame};
use super::kernel::Kernel;

/// 事件推送轮询间隔。
const EVENT_POLL: Duration = Duration::from_millis(50);

#[derive(Debug, Default)]
struct Subscription {
    topics: HashSet<String>,
    last_seq: u64,
}

/// 内核接口服务。
pub struct Server {
    kernel: Arc<Kernel>,
    events: Arc<EventBus>,
    token: String,
    listener: Mutex<Option<TcpListener>>,
    address: SocketAddr,
    shutdown: Arc<AtomicBool>,
    accept: Mutex<Option<JoinHandle<()>>>,
    connections: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl Server {
    /// 绑定到指定地址（`addr` 形如 `127.0.0.1:0`）。
    ///
    /// # Errors
    /// 绑定失败时返回 [`Error::Io`]。
    pub fn bind(kernel: Arc<Kernel>, addr: &str, token: impl Into<String>) -> Result<Self> {
        let listener = TcpListener::bind(addr)?;
        let address = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let events = Arc::clone(&kernel.events);
        Ok(Self {
            kernel,
            events,
            token: token.into(),
            listener: Mutex::new(Some(listener)),
            address,
            shutdown: Arc::new(AtomicBool::new(false)),
            accept: Mutex::new(None),
            connections: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// 监听地址。
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }

    /// 访问令牌（空字符串表示不校验）。
    pub fn token(&self) -> &str {
        &self.token
    }

    /// 启动接受循环。
    pub fn start(&self) {
        let Some(listener) = lock(&self.listener).take() else {
            return;
        };
        let kernel = Arc::clone(&self.kernel);
        let events = Arc::clone(&self.events);
        let token = self.token.clone();
        let shutdown = Arc::clone(&self.shutdown);
        let connections = Arc::clone(&self.connections);
        let sink = Arc::clone(&connections);
        let handle = thread::Builder::new()
            .name("agentheart-accept".to_string())
            .spawn(move || accept_loop(listener, kernel, events, token, shutdown, &sink));
        if let Ok(handle) = handle {
            *lock(&self.accept) = Some(handle);
        }
    }

    /// 关闭服务并等待接受线程退出（幂等）。
    ///
    /// 说明：客户端连接线程可能阻塞在读取上，此处**不 join**，直接分离句柄，
    /// 避免关闭被空闲连接阻塞。
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(handle) = lock(&self.accept).take() {
            let _ = handle.join();
        }
        lock(&self.connections).clear();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn accept_loop(
    listener: TcpListener,
    kernel: Arc<Kernel>,
    events: Arc<EventBus>,
    token: String,
    shutdown: Arc<AtomicBool>,
    connections: &Arc<Mutex<Vec<JoinHandle<()>>>>,
) {
    while !shutdown.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => {
                let kernel = Arc::clone(&kernel);
                let events = Arc::clone(&events);
                let token = token.clone();
                let spawned = thread::Builder::new()
                    .name("agentheart-conn".to_string())
                    .spawn(move || handle_connection(stream, &kernel, &events, &token));
                if let Ok(handle) = spawned {
                    lock(connections).push(handle);
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(_) => break,
        }
    }
}

fn handle_connection(stream: TcpStream, kernel: &Arc<Kernel>, events: &Arc<EventBus>, token: &str) {
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_nodelay(true);
    let Ok(mut writer_stream) = stream.try_clone() else {
        return;
    };
    let mut reader_stream = stream;
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let subscription = Arc::new(Mutex::new(Subscription::default()));
    let writer = {
        let events = Arc::clone(events);
        let subscription = Arc::clone(&subscription);
        thread::Builder::new()
            .name("agentheart-writer".to_string())
            .spawn(move || writer_loop(&mut writer_stream, &rx, &events, &subscription))
    };
    reader_loop(
        &mut reader_stream,
        kernel,
        events,
        token,
        &tx,
        &subscription,
    );
    drop(tx);
    if let Ok(handle) = writer {
        let _ = handle.join();
    }
}

fn reader_loop(
    stream: &mut TcpStream,
    kernel: &Arc<Kernel>,
    events: &Arc<EventBus>,
    token: &str,
    tx: &mpsc::Sender<Vec<u8>>,
    subscription: &Arc<Mutex<Subscription>>,
) {
    let mut authenticated = false;
    loop {
        let incoming = match frame::read_frame(stream) {
            Ok(frame) => frame,
            Err(_) => return,
        };
        let Ok(request) = json::parse_bytes(&incoming.payload) else {
            send_error(
                tx,
                incoming.request_id,
                2,
                "BAD_FRAME",
                "invalid json payload",
            );
            continue;
        };
        let name = request.get("m").and_then(Value::as_str).unwrap_or("");
        if !authenticated {
            if name != "system.hello" {
                send_error(
                    tx,
                    incoming.request_id,
                    4,
                    "UNAUTHORIZED",
                    "handshake required",
                );
                return;
            }
            let provided = request.get("token").and_then(Value::as_str).unwrap_or("");
            if !token.is_empty() && provided != token {
                send_error(tx, incoming.request_id, 4, "UNAUTHORIZED", "bad token");
                return;
            }
            authenticated = true;
        }
        if name == "stream.subscribe" {
            let from_seq = request
                .get("fromSeq")
                .and_then(Value::as_f64)
                .unwrap_or(0.0) as u64;
            let topics: HashSet<String> = request
                .get("topics")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_else(|| HashSet::from(["*".to_string()]));
            let latest = events.latest_seq();
            {
                let mut sub = lock(subscription);
                sub.topics = topics;
                sub.last_seq = from_seq;
            }
            let mut result = Value::object();
            result.insert("streamId", Value::String("s-1".to_string()));
            result.insert("seq", Value::Number(latest as f64));
            let mut response = Value::object();
            response.insert("m", Value::String("stream.subscribed".to_string()));
            response.insert("ok", Value::Bool(true));
            response.insert("result", result);
            send_response(tx, incoming.request_id, &response);
            continue;
        }
        let response = kernel.handle(&request);
        send_response(tx, incoming.request_id, &response);
    }
}

fn writer_loop(
    stream: &mut TcpStream,
    rx: &mpsc::Receiver<Vec<u8>>,
    events: &Arc<EventBus>,
    subscription: &Arc<Mutex<Subscription>>,
) {
    loop {
        match rx.recv_timeout(EVENT_POLL) {
            Ok(bytes) => {
                if stream.write_all(&bytes).is_err() {
                    return;
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
        let (topics, last_seq) = {
            let sub = lock(subscription);
            (sub.topics.clone(), sub.last_seq)
        };
        if topics.is_empty() {
            continue;
        }
        let mut newest = last_seq;
        for event in events.poll_after(last_seq) {
            newest = event.seq;
            if !topics.contains("*") && !topics.contains(&event.kind) {
                continue;
            }
            let payload = event.to_value().to_json_string();
            let Ok(bytes) = Frame::event(payload.into_bytes()).encode() else {
                continue;
            };
            if stream.write_all(&bytes).is_err() {
                return;
            }
        }
        if newest != last_seq {
            lock(subscription).last_seq = newest;
        }
    }
}

fn send_response(tx: &mpsc::Sender<Vec<u8>>, request_id: u32, value: &Value) {
    let frame = Frame::response(request_id, value.to_json_string().into_bytes());
    if let Ok(bytes) = frame.encode() {
        let _ = tx.send(bytes);
    }
}

fn send_error(
    tx: &mpsc::Sender<Vec<u8>>,
    request_id: u32,
    code: i32,
    code_name: &str,
    message: &str,
) {
    let mut error = Value::object();
    error.insert("code", Value::Number(f64::from(code)));
    error.insert("codeName", Value::String(code_name.to_string()));
    error.insert("message", Value::String(message.to_string()));
    let mut value = Value::object();
    value.insert("ok", Value::Bool(false));
    value.insert("error", error);
    let frame = Frame::error(request_id, value.to_json_string().into_bytes());
    if let Ok(bytes) = frame.encode() {
        let _ = tx.send(bytes);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
