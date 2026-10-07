// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! 测试用极简 MCP stdio 服务端（仅用于验证 [`agentheart_adapters::McpAdapter`]）。
//!
//! 支持 `initialize` 与 `tools/call`（回显工具名与参数）。不参与生产场景。

use std::io::{self, BufRead, Write};

use agentheart_core::json::{self, Value};

fn main() {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = stdout.lock();
    for line in stdin.lock().lines() {
        let Ok(line) = line else {
            break;
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(message) = json::parse(trimmed) else {
            continue;
        };
        // 通知（无 id）忽略
        let Some(id) = message.get("id").and_then(Value::as_f64) else {
            continue;
        };
        let method = message.get("method").and_then(Value::as_str).unwrap_or("");
        let reply = match method {
            "initialize" => Some(result_response(id, &initialize_result())),
            "tools/call" => Some(result_response(id, &tools_call_result(&message))),
            _ => Some(error_response(id, -32601, "method not found")),
        };
        if let Some(reply) = reply {
            let _ = writeln!(out, "{}", reply.to_json_string());
            let _ = out.flush();
        }
    }
}

fn initialize_result() -> Value {
    let mut server = Value::object();
    server.insert("name", Value::String("ah-fake-mcp".to_string()));
    server.insert("version", Value::String("0.1.0".to_string()));
    let mut capabilities = Value::object();
    capabilities.insert("tools", Value::object());
    let mut result = Value::object();
    result.insert("protocolVersion", Value::String("2024-11-05".to_string()));
    result.insert("capabilities", capabilities);
    result.insert("serverInfo", server);
    result
}

fn tools_call_result(message: &Value) -> Value {
    let name = message
        .get("params")
        .and_then(|params| params.get("name"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let mut text = Value::object();
    text.insert("type", Value::String("text".to_string()));
    text.insert("text", Value::String(format!("echo:{name}")));
    let mut result = Value::object();
    result.insert("content", Value::Array(vec![text]));
    result.insert("isError", Value::Bool(false));
    result
}

fn result_response(id: f64, result: &Value) -> Value {
    let mut value = Value::object();
    value.insert("jsonrpc", Value::String("2.0".to_string()));
    value.insert("id", Value::Number(id));
    value.insert("result", result.clone());
    value
}

fn error_response(id: f64, code: i32, message: &str) -> Value {
    let mut error = Value::object();
    error.insert("code", Value::Number(f64::from(code)));
    error.insert("message", Value::String(message.to_string()));
    let mut value = Value::object();
    value.insert("jsonrpc", Value::String("2.0".to_string()));
    value.insert("id", Value::Number(id));
    value.insert("error", error);
    value
}
