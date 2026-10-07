//! MQTT 3.1.1 子集桥接：把内核消息队列映射为 MQTT topic，供外部客户端对接。
//!
//! ## 映射约定
//!
//! **MQTT topic == 内核队列名**：
//! - 客户端 PUBLISH 到 topic `T` → `Broker::publish(T, payload)`；
//! - 客户端 SUBSCRIBE 过滤器 `F` → 桥接线程从**匹配 `F` 的队列**租约消息并推送。
//!
//! ## 支持的控制报文
//!
//! CONNECT/CONNACK、PUBLISH（双向）、PUBACK（双向）、SUBSCRIBE/SUBACK、
//! UNSUBSCRIBE/UNSUBACK、PINGREQ/PINGRESP、DISCONNECT。
//!
//! ## QoS 支持
//!
//! - **上行**：QoS 0/1（QoS 1 在消息入队后回 PUBACK）；
//! - **下行**：QoS 0 与 **QoS 1**（含单连接 inflight 表、包 ID 分配、DUP 重传）。
//!
//! ## 子集限制（有意为之）
//!
//! - 不支持 QoS 2、RETAIN、持久会话（按 clean session 处理）；
//! - **支持遗嘱（Will）**：连接异常结束（非主动 DISCONNECT）时发布；Will QoS 降级至 1、RETAIN 忽略；
//! - 消息体按 UTF-8 文本处理（与 `queue.publish` 的 `body: string` 一致）；
//! - 同一桥接实例内为 **fan-out**（所有匹配订阅者都收到）；但与其它消费者
//!   （如 `queue.lease` 客户端）之间是**竞争消费**关系。

use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::error::Result;
use crate::observe::log;
use crate::queue::Broker;

use super::packet::{self, connack_code, kind};

/// 桥接线程扫描队列与重传的间隔。
const PUMP_INTERVAL: Duration = Duration::from_millis(30);
/// 单轮每队列最多取走的消息数（保证公平）。
const MAX_DRAIN_PER_QUEUE: usize = 64;
/// 单连接出站缓冲上限。
const OUTBOUND_CAPACITY: usize = 256;
/// 单连接 QoS1 在途上限。
const MAX_INFLIGHT: usize = 32;
/// 接受循环在无新连接时的休眠。
const ACCEPT_IDLE: Duration = Duration::from_millis(20);
/// QoS1 下行默认重传间隔。
const DEFAULT_RETRANSMIT: Duration = Duration::from_secs(5);

/// 一条待确认的 QoS1 下行消息。
#[derive(Debug)]
struct PendingPublish {
    packet_id: u16,
    topic: String,
    payload: Vec<u8>,
    last_sent: Instant,
    attempts: u32,
}

/// 单连接的 QoS1 在途状态。
#[derive(Debug, Default)]
struct InFlight {
    next_id: u16,
    pending: Vec<PendingPublish>,
}

impl InFlight {
    /// 分配一个未被占用的包 ID。
    fn alloc_id(&mut self) -> Option<u16> {
        for _ in 0..u16::MAX {
            self.next_id = if self.next_id == u16::MAX {
                1
            } else {
                self.next_id + 1
            };
            if !self
                .pending
                .iter()
                .any(|pending| pending.packet_id == self.next_id)
            {
                return Some(self.next_id);
            }
        }
        None
    }

    /// 登记一条在途消息；超过上限返回 `false`。
    fn insert(&mut self, packet_id: u16, topic: &str, payload: &[u8]) -> bool {
        if self.pending.len() >= MAX_INFLIGHT {
            return false;
        }
        self.pending.push(PendingPublish {
            packet_id,
            topic: topic.to_string(),
            payload: payload.to_vec(),
            last_sent: Instant::now(),
            attempts: 1,
        });
        true
    }

    /// 收到 PUBACK：移除在途记录。
    fn ack(&mut self, packet_id: u16) {
        self.pending
            .retain(|pending| pending.packet_id != packet_id);
    }

    /// 取出超过 `interval` 未确认、需要重传的条目（并刷新发送时间与次数）。
    fn due(&mut self, interval: Duration) -> Vec<(u16, String, Vec<u8>)> {
        let now = Instant::now();
        let mut out = Vec::new();
        for pending in &mut self.pending {
            if now.saturating_duration_since(pending.last_sent) >= interval {
                pending.last_sent = now;
                pending.attempts = pending.attempts.saturating_add(1);
                out.push((
                    pending.packet_id,
                    pending.topic.clone(),
                    pending.payload.clone(),
                ));
            }
        }
        out
    }
}

/// 一条客户端连接（出站通道 + QoS1 在途状态）。
#[derive(Debug)]
struct Connection {
    conn_id: u64,
    sink: SyncSender<Vec<u8>>,
    inflight: Mutex<InFlight>,
}

/// 一条订阅记录。
#[derive(Debug, Clone)]
struct Subscriber {
    conn_id: u64,
    filter: String,
    qos: u8,
    conn: Arc<Connection>,
}

/// 桥接共享状态（在服务端与各线程间共享）。
#[derive(Debug)]
struct SharedState {
    broker: Arc<Broker>,
    token: String,
    shutdown: AtomicBool,
    subscriptions: Mutex<Vec<Subscriber>>,
    active: Mutex<Vec<Arc<Connection>>>,
    connections: Mutex<Vec<JoinHandle<()>>>,
    next_conn_id: AtomicU64,
}

/// MQTT 子集服务端。
#[derive(Debug)]
pub struct MqttServer {
    shared: Arc<SharedState>,
    listener: Mutex<Option<TcpListener>>,
    address: SocketAddr,
    retransmit_interval: Duration,
    accept: Mutex<Option<JoinHandle<()>>>,
    pump: Mutex<Option<JoinHandle<()>>>,
}

impl MqttServer {
    /// 绑定到指定地址（`addr` 形如 `127.0.0.1:1883`；填 `0` 由系统分配端口）。
    ///
    /// `token` 非空时要求 CONNECT 的 **password**（或 username）与之相等。
    ///
    /// # Errors
    /// 绑定失败时返回 IO 错误。
    pub fn bind(broker: Arc<Broker>, addr: &str, token: impl Into<String>) -> Result<Self> {
        let listener = TcpListener::bind(addr)?;
        let address = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let shared = Arc::new(SharedState {
            broker,
            token: token.into(),
            shutdown: AtomicBool::new(false),
            subscriptions: Mutex::new(Vec::new()),
            active: Mutex::new(Vec::new()),
            connections: Mutex::new(Vec::new()),
            next_conn_id: AtomicU64::new(1),
        });
        Ok(Self {
            shared,
            listener: Mutex::new(Some(listener)),
            address,
            retransmit_interval: DEFAULT_RETRANSMIT,
            accept: Mutex::new(None),
            pump: Mutex::new(None),
        })
    }

    /// 设置 QoS1 下行重传间隔（默认 5s；下限 10ms）。
    pub fn with_retransmit_interval(mut self, interval: Duration) -> Self {
        self.retransmit_interval = interval.max(Duration::from_millis(10));
        self
    }

    /// 监听地址。
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }

    /// 启动接受循环与投递泵。
    pub fn start(&self) {
        let Some(listener) = lock(&self.listener).take() else {
            return;
        };
        let accept_shared = Arc::clone(&self.shared);
        let accept = thread::Builder::new()
            .name("agentheart-mqtt-accept".to_string())
            .spawn(move || accept_loop(listener, &accept_shared));
        if let Ok(handle) = accept {
            *lock(&self.accept) = Some(handle);
        }

        let pump_shared = Arc::clone(&self.shared);
        let retransmit = self.retransmit_interval;
        let pump = thread::Builder::new()
            .name("agentheart-mqtt-pump".to_string())
            .spawn(move || pump_loop(&pump_shared, retransmit));
        if let Ok(handle) = pump {
            *lock(&self.pump) = Some(handle);
        }
    }

    /// 关闭服务（幂等）：停止接受与投递，并分离既有连接。
    pub fn shutdown(&self) {
        self.shared.shutdown.store(true, Ordering::Release);
        if let Some(handle) = lock(&self.accept).take() {
            let _ = handle.join();
        }
        if let Some(handle) = lock(&self.pump).take() {
            let _ = handle.join();
        }
        lock(&self.shared.connections).clear();
        lock(&self.shared.subscriptions).clear();
        lock(&self.shared.active).clear();
    }
}

impl Drop for MqttServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn accept_loop(listener: TcpListener, shared: &Arc<SharedState>) {
    while !shared.shutdown.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => {
                let task_shared = Arc::clone(shared);
                let conn_id = shared.next_conn_id.fetch_add(1, Ordering::Relaxed);
                let spawned = thread::Builder::new()
                    .name(format!("agentheart-mqtt-{conn_id}"))
                    .spawn(move || handle_connection(stream, &task_shared, conn_id));
                match spawned {
                    Ok(handle) => lock(&shared.connections).push(handle),
                    Err(err) => log::error(&format!("启动 MQTT 连接线程失败: {err}")),
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_IDLE);
            }
            Err(_) => break,
        }
    }
}

fn handle_connection(stream: TcpStream, shared: &Arc<SharedState>, conn_id: u64) {
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_nodelay(true);
    let Ok(mut writer_stream) = stream.try_clone() else {
        return;
    };
    let mut reader_stream = stream;
    let (sink, outbound) = mpsc::sync_channel::<Vec<u8>>(OUTBOUND_CAPACITY);
    let connection = Arc::new(Connection {
        conn_id,
        sink,
        inflight: Mutex::new(InFlight::default()),
    });
    lock(&shared.active).push(Arc::clone(&connection));

    let writer = thread::Builder::new()
        .name(format!("agentheart-mqtt-writer-{conn_id}"))
        .spawn(move || writer_loop(&mut writer_stream, &outbound));

    if session_loop(&mut reader_stream, shared, &connection).is_err() {
        log::debug("MQTT 会话结束");
    }

    // 清理本连接的订阅与在途登记，并结束写线程
    lock(&shared.subscriptions).retain(|subscriber| subscriber.conn_id != conn_id);
    lock(&shared.active).retain(|item| item.conn_id != conn_id);
    drop(connection);
    if let Ok(handle) = writer {
        let _ = handle.join();
    }
}

/// 会话结束方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionEnd {
    /// 客户端主动 DISCONNECT（不触发遗嘱）。
    Disconnect,
    /// 连接异常结束（触发遗嘱）。
    Aborted,
}

/// 处理一条连接：CONNECT 握手 → 会话循环 → 异常结束时发布遗嘱。
fn session_loop(
    stream: &mut TcpStream,
    shared: &Arc<SharedState>,
    connection: &Arc<Connection>,
) -> Result<()> {
    let Some(connect) = accept_connect(stream, shared, connection) else {
        return Ok(());
    };
    if session_body(stream, shared, connection) == SessionEnd::Aborted {
        if let Some(will) = connect.will {
            publish_will(shared, &will);
        }
    }
    Ok(())
}

/// 读取并校验 CONNECT；失败时发送相应 CONNACK 并返回 `None`。
fn accept_connect(
    stream: &mut TcpStream,
    shared: &Arc<SharedState>,
    connection: &Arc<Connection>,
) -> Option<packet::ConnectData> {
    let first = packet::read_packet(stream).ok()?;
    if first.packet_type != kind::CONNECT {
        return None;
    }
    let connect = match packet::parse_connect(&first.payload) {
        Ok(connect) => connect,
        Err(error) => {
            log::warn(&format!("MQTT CONNECT 解析失败: {error}"));
            let _ = connection
                .sink
                .send(packet::connack(false, connack_code::BAD_PROTOCOL));
            return None;
        }
    };
    let username = connect.username.as_deref().unwrap_or("");
    let password = connect.password.as_deref().unwrap_or("");
    if !shared.token.is_empty() && password != shared.token && username != shared.token {
        let _ = connection
            .sink
            .send(packet::connack(false, connack_code::BAD_CREDENTIALS));
        return None;
    }
    log::debug(&format!(
        "MQTT 客户端已连接: client_id={} conn_id={}",
        connect.client_id, connection.conn_id
    ));
    if connection
        .sink
        .send(packet::connack(false, connack_code::ACCEPTED))
        .is_err()
    {
        return None;
    }
    Some(connect)
}

/// 会话主循环，返回结束方式。
fn session_body(
    stream: &mut TcpStream,
    shared: &Arc<SharedState>,
    connection: &Arc<Connection>,
) -> SessionEnd {
    loop {
        let Ok(incoming) = packet::read_packet(stream) else {
            return SessionEnd::Aborted;
        };
        match incoming.packet_type {
            kind::PUBLISH => {
                let Ok(published) = packet::parse_publish(incoming.flags, &incoming.payload) else {
                    return SessionEnd::Aborted;
                };
                let topic = published.topic.clone();
                let body = String::from_utf8_lossy(&published.payload).into_owned();
                if let Err(error) = shared.broker.publish(&topic, body) {
                    log::warn(&format!("MQTT 发布失败: {error}"));
                }
                if let Some(packet_id) = published.packet_id {
                    let _ = connection.sink.send(packet::puback(packet_id));
                }
            }
            kind::PUBACK => {
                if incoming.payload.len() >= 2 {
                    let packet_id = u16::from_be_bytes([incoming.payload[0], incoming.payload[1]]);
                    lock(&connection.inflight).ack(packet_id);
                }
            }
            kind::SUBSCRIBE => {
                let Ok((packet_id, filters)) = packet::parse_subscribe(&incoming.payload) else {
                    return SessionEnd::Aborted;
                };
                let mut codes = Vec::with_capacity(filters.len());
                {
                    let mut registry = lock(&shared.subscriptions);
                    for (filter, requested) in &filters {
                        let granted = (*requested).min(1);
                        registry.push(Subscriber {
                            conn_id: connection.conn_id,
                            filter: filter.clone(),
                            qos: granted,
                            conn: Arc::clone(connection),
                        });
                        codes.push(granted);
                    }
                }
                let _ = connection.sink.send(packet::suback(packet_id, &codes));
            }
            kind::UNSUBSCRIBE => {
                let Ok((packet_id, filters)) = packet::parse_unsubscribe(&incoming.payload) else {
                    return SessionEnd::Aborted;
                };
                lock(&shared.subscriptions).retain(|subscriber| {
                    subscriber.conn_id != connection.conn_id
                        || !filters.contains(&subscriber.filter)
                });
                let _ = connection.sink.send(packet::unsuback(packet_id));
            }
            kind::PINGREQ => {
                let _ = connection.sink.send(packet::pingresp());
            }
            kind::DISCONNECT => return SessionEnd::Disconnect,
            _ => return SessionEnd::Aborted,
        }
    }
}

/// 发布遗嘱消息（仅在连接异常结束时调用）。
fn publish_will(shared: &Arc<SharedState>, will: &packet::Will) {
    let body = String::from_utf8_lossy(&will.payload).into_owned();
    match shared.broker.publish(&will.topic, body) {
        Ok(_) => log::debug(&format!(
            "MQTT 遗嘱已发布: topic={} qos={} retain={}",
            will.topic, will.qos, will.retain
        )),
        Err(error) => log::warn(&format!("MQTT 遗嘱发布失败: {error}")),
    }
}

fn writer_loop(stream: &mut TcpStream, outbound: &mpsc::Receiver<Vec<u8>>) {
    while let Ok(bytes) = outbound.recv() {
        if stream.write_all(&bytes).is_err() {
            return;
        }
    }
}

/// 投递泵：租约匹配订阅的队列消息并 fan-out，同时负责 QoS1 重传。
fn pump_loop(shared: &Arc<SharedState>, retransmit: Duration) {
    while !shared.shutdown.load(Ordering::Acquire) {
        thread::sleep(PUMP_INTERVAL);
        retransmit_due(shared, retransmit);

        let snapshot: Vec<Subscriber> = lock(&shared.subscriptions).clone();
        if snapshot.is_empty() {
            continue;
        }
        for queue in shared.broker.queues() {
            if !snapshot
                .iter()
                .any(|subscriber| packet::topic_matches(&subscriber.filter, &queue))
            {
                continue;
            }
            let mut drained = 0_usize;
            while drained < MAX_DRAIN_PER_QUEUE {
                let Some(message) = shared.broker.lease(&queue, Duration::from_millis(0)) else {
                    break;
                };
                let payload = message.body.as_bytes();
                let mut delivered = false;
                for subscriber in &snapshot {
                    if !packet::topic_matches(&subscriber.filter, &queue) {
                        continue;
                    }
                    if deliver(subscriber, &queue, payload) {
                        delivered = true;
                    }
                }
                if delivered {
                    let _ = shared.broker.ack(&message.id);
                } else {
                    // 无人在线 / 下游积压：退回队列，避免丢消息
                    let _ = shared.broker.nack(&message.id, true);
                    break;
                }
                drained += 1;
            }
        }
    }
}

/// 向单个订阅者投递一条消息（按授予的 QoS 选择报文）。
fn deliver(subscriber: &Subscriber, topic: &str, payload: &[u8]) -> bool {
    if subscriber.qos == 0 {
        return matches!(
            subscriber
                .conn
                .sink
                .try_send(packet::publish_qos0(topic, payload)),
            Ok(())
        );
    }
    // QoS 1：先登记在途，再发送
    let packet_id = {
        let mut inflight = lock(&subscriber.conn.inflight);
        let Some(packet_id) = inflight.alloc_id() else {
            return false;
        };
        if !inflight.insert(packet_id, topic, payload) {
            return false;
        }
        packet_id
    };
    let bytes = packet::publish_qos1(topic, payload, packet_id, false);
    match subscriber.conn.sink.try_send(bytes) {
        Ok(()) => true,
        Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
            lock(&subscriber.conn.inflight).ack(packet_id);
            false
        }
    }
}

/// 对所有活跃连接执行 QoS1 重传（置 DUP 标志）。
fn retransmit_due(shared: &Arc<SharedState>, interval: Duration) {
    let connections: Vec<Arc<Connection>> = lock(&shared.active).clone();
    for connection in connections {
        for (packet_id, topic, payload) in lock(&connection.inflight).due(interval) {
            let bytes = packet::publish_qos1(&topic, &payload, packet_id, true);
            // 下游堵塞或断开：保留在途，等待下一轮重传
            let _ = connection.sink.try_send(bytes);
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
