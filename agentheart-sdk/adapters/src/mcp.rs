//! MCP 适配器：通过 **stdio** 调用 MCP（Model Context Protocol）服务器的工具。
//!
//! 传输：MCP stdio —— 每行一个 JSON-RPC 2.0 消息。每次任务执行：拉起 MCP 服务端进程 →
//! `initialize` → `notifications/initialized` → `tools/call` → 关闭进程。
//!
//! 工具的 `arguments` = 适配器配置的静态参数 + 任务上下文
//! （`taskId` / `queue` / `taskName?` / `attempts`）。
//!
//! 说明：本适配器按「一次任务一次会话」实现（简单、无状态）。
//! 读响应依赖服务端回包，若 MCP 服务端异常挂起，请为任务配置 `timeout`，
//! 由内核的执行超时（业务心跳 / 单次超时）负责中止并释放执行位。

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

use agentheart_core::Task;
use agentheart_core::json::{self, Value};

use crate::{AdapterError, AdapterResult, AgentAdapter};

/// 客户端声明的 MCP 协议版本。
const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
/// 客户端名称。
const CLIENT_NAME: &str = "agentheart";
/// 客户端版本。
const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// MCP 工具适配器。
#[derive(Debug, Clone)]
pub struct McpAdapter {
    name: String,
    program: String,
    args: Vec<String>,
    envs: Vec<(String, String)>,
    tool: String,
    arguments: Value,
}

impl McpAdapter {
    /// 以「MCP 服务端启动命令 + 工具名」构造。
    pub fn new(
        name: impl Into<String>,
        program: impl Into<String>,
        tool: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            program: program.into(),
            args: Vec::new(),
            envs: Vec::new(),
            tool: tool.into(),
            arguments: Value::object(),
        }
    }

    /// 追加 MCP 服务端启动参数。
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// 追加 MCP 服务端环境变量。
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.envs.push((key.into(), value.into()));
        self
    }

    /// 设置工具的静态参数（对象）。
    pub fn arguments(mut self, arguments: Value) -> Self {
        self.arguments = arguments;
        self
    }
}

impl AgentAdapter for McpAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn call(&self, task: &Task) -> AdapterResult<()> {
        let mut command = Command::new(&self.program);
        command
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (key, value) in &self.envs {
            command.env(key, value);
        }
        let mut child = command.spawn()?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| AdapterError::new("无法写入 MCP 子进程".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AdapterError::new("无法读取 MCP 子进程".to_string()))?;
        let mut reader = BufReader::new(stdout);

        let outcome = self.session(&mut stdin, &mut reader, task);

        // 关闭 stdin 让服务端优雅退出；兜底强杀并回收，避免僵尸进程
        drop(stdin);
        let _ = child.kill();
        let _ = child.wait();
        outcome
    }
}

impl McpAdapter {
    /// 一次完整会话：initialize → initialized 通知 → tools/call。
    fn session(
        &self,
        stdin: &mut impl Write,
        reader: &mut impl BufRead,
        task: &Task,
    ) -> AdapterResult<()> {
        write_message(stdin, &request(1, "initialize", &initialize_params()))?;
        let response = read_response(reader, 1)?;
        if let Some(error) = response.get("error") {
            return Err(AdapterError::new(format!(
                "MCP initialize 失败: {}",
                error.to_json_string()
            )));
        }

        let mut notification = Value::object();
        notification.insert("jsonrpc", Value::String("2.0".to_string()));
        notification.insert(
            "method",
            Value::String("notifications/initialized".to_string()),
        );
        write_message(stdin, &notification)?;

        let mut params = Value::object();
        params.insert("name", Value::String(self.tool.clone()));
        params.insert("arguments", self.arguments_for(task));
        write_message(stdin, &request(2, "tools/call", &params))?;
        let response = read_response(reader, 2)?;
        if let Some(error) = response.get("error") {
            return Err(AdapterError::new(format!(
                "MCP tools/call 失败: {}",
                error.to_json_string()
            )));
        }
        let result = response
            .get("result")
            .ok_or_else(|| AdapterError::new("MCP 响应缺少 result".to_string()))?;
        if result.get("isError").and_then(Value::as_bool) == Some(true) {
            return Err(AdapterError::new(format!(
                "MCP 工具返回错误: {}",
                result.to_json_string()
            )));
        }
        Ok(())
    }

    /// 合并静态参数与任务上下文。
    fn arguments_for(&self, task: &Task) -> Value {
        let mut arguments = Value::object();
        if let Some(entries) = self.arguments.as_object() {
            for (key, value) in entries {
                arguments.insert(key.clone(), value.clone());
            }
        }
        arguments.insert("taskId", Value::String(task.id.to_string()));
        arguments.insert("queue", Value::String(task.queue.clone()));
        if let Some(name) = &task.name {
            arguments.insert("taskName", Value::String(name.clone()));
        }
        arguments.insert("attempts", Value::Number(f64::from(task.attempts)));
        arguments
    }
}

/// 构造 JSON-RPC 请求。
fn request(id: u64, method: &str, params: &Value) -> Value {
    let mut value = Value::object();
    value.insert("jsonrpc", Value::String("2.0".to_string()));
    value.insert("id", Value::Number(id as f64));
    value.insert("method", Value::String(method.to_string()));
    value.insert("params", params.clone());
    value
}

/// `initialize` 参数。
fn initialize_params() -> Value {
    let mut client = Value::object();
    client.insert("name", Value::String(CLIENT_NAME.to_string()));
    client.insert("version", Value::String(CLIENT_VERSION.to_string()));
    let mut params = Value::object();
    params.insert(
        "protocolVersion",
        Value::String(MCP_PROTOCOL_VERSION.to_string()),
    );
    params.insert("capabilities", Value::object());
    params.insert("clientInfo", client);
    params
}

/// 写入一条 JSON-RPC 消息（换行分隔）。
fn write_message(writer: &mut impl Write, message: &Value) -> AdapterResult<()> {
    writer.write_all(message.to_json_string().as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

/// 读取指定 `id` 的响应（跳过通知与其它消息）。
fn read_response(reader: &mut impl BufRead, id: u64) -> AdapterResult<Value> {
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Err(AdapterError::new(format!(
                "MCP 服务端提前关闭（等待响应 id={id}）"
            )));
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = json::parse(trimmed) else {
            continue;
        };
        match value.get("id").and_then(Value::as_f64) {
            Some(found) if found as u64 == id => return Ok(value),
            _ => continue,
        }
    }
}
