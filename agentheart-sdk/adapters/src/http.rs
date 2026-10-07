//! HTTP 适配器：把任务映射为一次 HTTP 请求（内置极简 HTTP/1.1 客户端，无第三方依赖）。

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use agentheart_core::Task;
use agentheart_core::json::Value;

use crate::{AdapterError, AdapterResult, AgentAdapter};

/// 响应读取上限（防恶意/失控响应）。
const MAX_RESPONSE: usize = 64 * 1024;

/// HTTP 适配器：向配置的 URL 发起请求，请求体为任务上下文的 JSON。
#[derive(Debug, Clone)]
pub struct HttpAdapter {
    name: String,
    host: String,
    port: u16,
    path: String,
    method: String,
    headers: Vec<(String, String)>,
    timeout: Duration,
}

impl HttpAdapter {
    /// 由 `http://host:port/path` 构造（仅支持 http，缺省端口 80、路径 `/`）。
    ///
    /// # Errors
    /// URL 不以 `http://` 开头或 host 缺失时返回 [`AdapterError`]。
    pub fn new(name: impl Into<String>, url: &str) -> AdapterResult<Self> {
        let rest = url
            .strip_prefix("http://")
            .ok_or_else(|| AdapterError::new(format!("仅支持 http:// 前缀: {url}")))?;
        let (authority, path) = match rest.split_once('/') {
            Some((authority, tail)) => (authority, format!("/{tail}")),
            None => (rest, "/".to_string()),
        };
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) => (
                host.to_string(),
                port.parse::<u16>()
                    .map_err(|_| AdapterError::new(format!("端口非法: {port}")))?,
            ),
            None => (authority.to_string(), 80),
        };
        if host.is_empty() {
            return Err(AdapterError::new(format!("缺少主机名: {url}")));
        }
        Ok(Self {
            name: name.into(),
            host,
            port,
            path,
            method: "POST".to_string(),
            headers: Vec::new(),
            timeout: Duration::from_secs(10),
        })
    }

    /// 设置请求方法（默认 `POST`）。
    pub fn method(mut self, method: impl Into<String>) -> Self {
        self.method = method.into();
        self
    }

    /// 追加请求头。
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    /// 设置连接 / 读写超时。
    pub fn timeout(mut self, limit: Duration) -> Self {
        self.timeout = limit;
        self
    }
}

impl AgentAdapter for HttpAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn call(&self, task: &Task) -> AdapterResult<()> {
        let body = task_json(task);
        let address = (self.host.as_str(), self.port)
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| {
                AdapterError::new(format!("无法解析地址: {}:{}", self.host, self.port))
            })?;
        let mut stream = TcpStream::connect_timeout(&address, self.timeout)?;
        stream.set_read_timeout(Some(self.timeout))?;
        stream.set_write_timeout(Some(self.timeout))?;

        let mut request = format!(
            "{} {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
            self.method,
            self.path,
            self.host,
            body.len()
        );
        for (key, value) in &self.headers {
            request.push_str(&format!("{key}: {value}\r\n"));
        }
        request.push_str("\r\n");
        request.push_str(&body);

        stream.write_all(request.as_bytes())?;
        stream.flush()?;

        let mut response = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            let read = stream.read(&mut chunk)?;
            if read == 0 {
                break;
            }
            response.extend_from_slice(&chunk[..read]);
            if response.len() >= MAX_RESPONSE {
                break;
            }
        }

        let status = parse_status(&response)
            .ok_or_else(|| AdapterError::new("响应缺少状态行".to_string()))?;
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(AdapterError::new(format!("HTTP 状态码 {status}")))
        }
    }
}

/// 任务上下文的 JSON（沿用内核自研 JSON 编解码，确保转义正确）。
fn task_json(task: &Task) -> String {
    let mut value = Value::object();
    value.insert("id", Value::String(task.id.to_string()));
    value.insert("queue", Value::String(task.queue.clone()));
    if let Some(name) = &task.name {
        value.insert("name", Value::String(name.clone()));
    }
    value.insert("attempts", Value::Number(f64::from(task.attempts)));
    if let Some(payload) = &task.payload_ref {
        value.insert("payloadRef", Value::String(payload.clone()));
    }
    value.to_json_string()
}

/// 解析状态行中的状态码（`HTTP/1.1 200 OK`）。
fn parse_status(response: &[u8]) -> Option<u16> {
    let text = String::from_utf8_lossy(response);
    let line = text.lines().next()?;
    let mut parts = line.split_whitespace();
    let _version = parts.next()?;
    parts.next()?.parse().ok()
}
