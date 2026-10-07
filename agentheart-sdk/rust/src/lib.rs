//! AgentHeart Rust SDK：内核复用 + 内核接口会话（Socket / 内嵌）。
//!
//! - [`Session::connect`]：连接侧车（`agentheartd`）内核接口（帧协议 + 事件流）；
//! - [`Session::embedded`]：进程内直调内核（无需网络）；
//! - 统一调用面：[`Session::call`] / [`Session::subscribe`] / [`Session::drain_events`]。
//!
//! ## 事件流
//!
//! Socket 模式下由后台读线程**并发**收取事件帧（按请求 ID 分发响应、其余入事件队列），
//! 因此一次连接即可同时完成请求-响应与事件订阅；内嵌模式直接读取内核 `EventBus`。

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub use agentheart_core;
use agentheart_core::json::{self, Value};
use agentheart_core::protocol::{Frame, flags, read_frame, write_frame};
use agentheart_core::{EventBus, Kernel};

/// 单连接事件队列上限（超出丢弃最旧事件）。
const MAX_EVENTS: usize = 1024;

/// 内核接口会话（Socket 或内嵌）。
#[derive(Debug)]
pub struct Session {
    inner: Inner,
}

#[derive(Debug)]
enum Inner {
    Socket(SocketClient),
    Embedded(EmbeddedClient),
}

impl Session {
    /// 连接侧车内核接口并完成握手。
    ///
    /// # Errors
    /// 连接失败、握手被拒或响应非法时返回错误。
    pub fn connect(addr: SocketAddr, token: &str, timeout: Duration) -> Result<Self, io::Error> {
        let mut client = SocketClient::connect(addr, timeout)?;
        let hello = format!("{{\"m\":\"system.hello\",\"ver\":1,\"token\":\"{token}\"}}");
        let response = client.call(&hello)?;
        if response.get("ok").and_then(Value::as_bool) != Some(true) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "handshake rejected",
            ));
        }
        Ok(Self {
            inner: Inner::Socket(client),
        })
    }

    /// 以内嵌内核建立会话（进程内直调，无需网络）。
    pub fn embedded(kernel: Arc<Kernel>) -> Self {
        let events = Arc::clone(&kernel.events);
        Self {
            inner: Inner::Embedded(EmbeddedClient {
                kernel,
                events,
                topics: Vec::new(),
                cursor: 0,
            }),
        }
    }

    /// 承载模式：`socket`（侧车）或 `embedded`（内嵌）。
    pub fn mode(&self) -> &'static str {
        match self.inner {
            Inner::Socket(_) => "socket",
            Inner::Embedded(_) => "embedded",
        }
    }

    /// 发送一次请求并等待响应。
    ///
    /// # Errors
    /// 连接已断开、写入失败或响应超时时返回错误。
    pub fn call(&mut self, request: &str) -> Result<Value, io::Error> {
        match &mut self.inner {
            Inner::Socket(client) => client.call(request),
            Inner::Embedded(client) => client.call(request),
        }
    }

    /// 订阅事件流（`topics` 为空或含 `"*"` 表示全部）。
    ///
    /// # Errors
    /// Socket 模式订阅失败时返回错误。
    pub fn subscribe(&mut self, topics: &[&str], from_seq: u64) -> Result<Value, io::Error> {
        match &mut self.inner {
            Inner::Socket(client) => client.subscribe(topics, from_seq),
            Inner::Embedded(client) => Ok(client.subscribe(topics, from_seq)),
        }
    }

    /// 拉取自上次拉取以来收到的事件（非阻塞，返回 `event.*` JSON 值）。
    pub fn drain_events(&mut self) -> Vec<Value> {
        match &mut self.inner {
            Inner::Socket(client) => client.drain_events(),
            Inner::Embedded(client) => client.drain_events(),
        }
    }
}

// ---- Socket 承载 ----

#[derive(Debug)]
struct SocketClient {
    stream: TcpStream,
    pending: Arc<Mutex<HashMap<u32, Sender<Value>>>>,
    events: Arc<Mutex<VecDeque<Value>>>,
    closed: Arc<AtomicBool>,
    next_id: u32,
    timeout: Duration,
    reader: Option<JoinHandle<()>>,
}

impl SocketClient {
    fn connect(addr: SocketAddr, timeout: Duration) -> Result<Self, io::Error> {
        let stream = TcpStream::connect_timeout(&addr, timeout)?;
        stream.set_read_timeout(None)?;
        stream.set_write_timeout(Some(timeout))?;
        stream.set_nodelay(true)?;
        let mut reader_stream = stream.try_clone()?;

        let pending = Arc::new(Mutex::new(HashMap::new()));
        let events = Arc::new(Mutex::new(VecDeque::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let reader = {
            let pending = Arc::clone(&pending);
            let events = Arc::clone(&events);
            let closed = Arc::clone(&closed);
            thread::Builder::new()
                .name("agentheart-sdk-reader".to_string())
                .spawn(move || reader_loop(&mut reader_stream, &pending, &events, &closed))?
        };

        Ok(Self {
            stream,
            pending,
            events,
            closed,
            next_id: 1,
            timeout,
            reader: Some(reader),
        })
    }

    fn call(&mut self, request: &str) -> Result<Value, io::Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "kernel connection is closed",
            ));
        }
        let request_id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);

        let (sender, receiver) = mpsc::channel();
        lock(&self.pending).insert(request_id, sender);
        let frame = Frame::request(request_id, request.as_bytes().to_vec());
        if let Err(error) = write_frame(&mut self.stream, &frame) {
            lock(&self.pending).remove(&request_id);
            return Err(io::Error::other(error.to_string()));
        }
        match receiver.recv_timeout(self.timeout) {
            Ok(value) => Ok(value),
            Err(_) => {
                lock(&self.pending).remove(&request_id);
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "kernel response timeout",
                ))
            }
        }
    }

    fn subscribe(&mut self, topics: &[&str], from_seq: u64) -> Result<Value, io::Error> {
        let list = topics
            .iter()
            .map(|topic| format!("\"{topic}\""))
            .collect::<Vec<_>>()
            .join(",");
        self.call(&format!(
            "{{\"m\":\"stream.subscribe\",\"topics\":[{list}],\"fromSeq\":{from_seq}}}"
        ))
    }

    fn drain_events(&mut self) -> Vec<Value> {
        lock(&self.events).drain(..).collect()
    }
}

impl Drop for SocketClient {
    fn drop(&mut self) {
        let _ = self.stream.shutdown(Shutdown::Both);
        if let Some(handle) = self.reader.take() {
            let _ = handle.join();
        }
    }
}

/// 后台读线程：事件帧入队，响应帧按请求 ID 分发；读失败即标记连接关闭并唤醒等待者。
fn reader_loop(
    stream: &mut TcpStream,
    pending: &Arc<Mutex<HashMap<u32, Sender<Value>>>>,
    events: &Arc<Mutex<VecDeque<Value>>>,
    closed: &Arc<AtomicBool>,
) {
    while let Ok(frame) = read_frame(stream) {
        let Ok(value) = json::parse_bytes(&frame.payload) else {
            continue;
        };
        if frame.flags & flags::EVENT != 0 {
            let mut queue = lock(events);
            queue.push_back(value);
            while queue.len() > MAX_EVENTS {
                queue.pop_front();
            }
        } else if let Some(sender) = lock(pending).remove(&frame.request_id) {
            let _ = sender.send(value);
        }
    }
    closed.store(true, Ordering::Release);
    // 丢弃所有未决请求的发送端，令等待者立即返回错误
    lock(pending).clear();
}

// ---- 内嵌承载 ----

#[derive(Debug)]
struct EmbeddedClient {
    kernel: Arc<Kernel>,
    events: Arc<EventBus>,
    topics: Vec<String>,
    cursor: u64,
}

impl EmbeddedClient {
    fn call(&mut self, request: &str) -> Result<Value, io::Error> {
        let value = json::parse(request).map_err(|error| io::Error::other(error.to_string()))?;
        Ok(self.kernel.handle(&value))
    }

    fn subscribe(&mut self, topics: &[&str], from_seq: u64) -> Value {
        self.topics = topics.iter().map(|topic| (*topic).to_string()).collect();
        self.cursor = from_seq;
        let mut result = Value::object();
        result.insert("streamId", Value::String("embedded".to_string()));
        result.insert("seq", Value::Number(self.events.latest_seq() as f64));
        let mut value = Value::object();
        value.insert("m", Value::String("stream.subscribed".to_string()));
        value.insert("ok", Value::Bool(true));
        value.insert("result", result);
        value
    }

    fn drain_events(&mut self) -> Vec<Value> {
        let mut out = Vec::new();
        for event in self.events.poll_after(self.cursor) {
            self.cursor = event.seq;
            let wanted = self.topics.is_empty()
                || self
                    .topics
                    .iter()
                    .any(|topic| topic == "*" || topic == &event.kind);
            if wanted {
                out.push(event.to_value());
            }
        }
        out
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::Session;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use agentheart_core::{
        Broker, BrokerConfig, EventBus, Handler, Kernel, Scheduler, SchedulerConfig, Server,
        SystemClock, Task,
    };

    fn build_kernel() -> (Arc<Scheduler>, Arc<Kernel>) {
        let handler: Handler = Arc::new(|_task: &Task| Ok(()));
        let config = SchedulerConfig {
            workers: 1,
            heartbeat_interval: Duration::from_secs(600),
            adaptive_heartbeat: false,
            ..SchedulerConfig::default()
        };
        let scheduler = Arc::new(Scheduler::start_with(config, handler));
        let broker = Arc::new(Broker::new(BrokerConfig::default(), Arc::new(SystemClock)));
        let events = Arc::new(EventBus::new(256));
        let kernel = Arc::new(Kernel::new(
            Arc::clone(&scheduler),
            broker,
            Arc::clone(&events),
        ));
        (scheduler, kernel)
    }

    fn wait_until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !condition() {
            assert!(Instant::now() < deadline, "条件未在期限内满足");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    fn contains_state(events: &[agentheart_core::json::Value], state: &str) -> bool {
        events.iter().any(|event| {
            event
                .get("m")
                .and_then(agentheart_core::json::Value::as_str)
                == Some("event.task")
                && event
                    .get("state")
                    .and_then(agentheart_core::json::Value::as_str)
                    == Some(state)
        })
    }

    #[test]
    fn should_call_kernel_and_stream_events_over_socket() {
        let (scheduler, kernel) = build_kernel();
        let server = Server::bind(Arc::clone(&kernel), "127.0.0.1:0", "token-1").expect("bind");
        server.start();

        let mut session =
            Session::connect(server.local_addr(), "token-1", Duration::from_secs(5)).expect("连接");
        assert_eq!(session.mode(), "socket");

        // 请求-响应
        let health = session.call(r#"{"m":"system.health"}"#).expect("调用成功");
        assert_eq!(
            health
                .get("ok")
                .and_then(agentheart_core::json::Value::as_bool),
            Some(true)
        );

        // 订阅事件流（仅 task 主题）
        let subscribed = session.subscribe(&["task"], 0).expect("订阅成功");
        assert_eq!(
            subscribed
                .get("ok")
                .and_then(agentheart_core::json::Value::as_bool),
            Some(true)
        );

        // 触发任务 → 在事件队列中收到 event.task
        scheduler.submit(Task::new("q")).expect("提交成功");
        wait_until(|| contains_state(&session.drain_events(), "succeeded"));

        // 请求-响应在订阅后仍可用（读线程按 ID 分发）
        let metrics = session.call(r#"{"m":"metrics.get"}"#).expect("调用成功");
        assert_eq!(
            metrics
                .get("ok")
                .and_then(agentheart_core::json::Value::as_bool),
            Some(true)
        );

        server.shutdown();
        scheduler.shutdown();
    }

    #[test]
    fn should_call_and_stream_events_when_embedded() {
        let (scheduler, kernel) = build_kernel();
        let mut session = Session::embedded(Arc::clone(&kernel));
        assert_eq!(session.mode(), "embedded");

        let health = session.call(r#"{"m":"system.health"}"#).expect("调用成功");
        assert_eq!(
            health
                .get("ok")
                .and_then(agentheart_core::json::Value::as_bool),
            Some(true)
        );

        session.subscribe(&["*"], 0).expect("订阅成功");
        scheduler.submit(Task::new("q")).expect("提交成功");
        wait_until(|| contains_state(&session.drain_events(), "succeeded"));

        scheduler.shutdown();
    }

    #[test]
    fn should_ignore_events_outside_subscribed_topics() {
        let (scheduler, kernel) = build_kernel();
        let mut session = Session::embedded(Arc::clone(&kernel));
        session.subscribe(&["delivery"], 0).expect("订阅成功");

        scheduler.submit(Task::new("q")).expect("提交成功");
        wait_until(|| scheduler.stats().succeeded >= 1);
        std::thread::sleep(Duration::from_millis(20));

        let events = session.drain_events();
        assert!(
            events.iter().all(|event| {
                event
                    .get("m")
                    .and_then(agentheart_core::json::Value::as_str)
                    != Some("event.task")
            }),
            "未订阅的 task 事件不应被投递: {events:?}"
        );
        scheduler.shutdown();
    }
}
