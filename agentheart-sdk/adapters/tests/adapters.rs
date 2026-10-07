//! 适配器集成测试：命令行 / HTTP / MCP 三类动作，以及 Handler 与路由封装。

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use agentheart_adapters::{
    AgentAdapter, CommandAdapter, DynAdapter, HttpAdapter, McpAdapter, handler as into_handler,
    router,
};
use agentheart_core::Task;
use agentheart_core::json::Value;

fn task(queue: &str) -> Task {
    Task::new(queue).name("demo")
}

// ---- 命令行 ----

#[test]
fn should_run_command_and_report_failure() {
    let ok = CommandAdapter::from_shell("ok", "exit 0");
    assert!(ok.call(&task("build")).is_ok(), "退出码 0 应视为成功");

    let bad = CommandAdapter::from_shell("bad", "exit 3");
    let error = bad.call(&task("build")).expect_err("非零退出码应失败");
    assert!(
        error.to_string().contains("退出码"),
        "错误应说明退出码: {error}"
    );
}

#[test]
fn should_kill_command_on_timeout() {
    let command = if cfg!(windows) {
        "ping -n 6 127.0.0.1 > nul"
    } else {
        "sleep 5"
    };
    let slow = CommandAdapter::from_shell("slow", command).timeout(Duration::from_millis(100));
    let error = slow.call(&task("build")).expect_err("超时应失败");
    assert!(
        error.to_string().contains("超时"),
        "错误应说明超时: {error}"
    );
}

#[test]
fn should_inject_task_context_into_command_environment() {
    // 交由 shell 判定：仅当任务队列被注入为环境变量时才以 0 退出（避免任何文件读写）
    let command = if cfg!(windows) {
        "if \"%AGENTHEART_TASK_QUEUE%\"==\"env-queue\" (exit 0) else (exit 1)"
    } else {
        "[ \"$AGENTHEART_TASK_QUEUE\" = \"env-queue\" ]"
    };
    CommandAdapter::from_shell("env", command)
        .call(&task("env-queue"))
        .expect("应把任务队列注入子进程环境变量");
}

// ---- HTTP ----

/// 启动一次性 HTTP 服务，返回地址与状态码。
fn spawn_http_server(status: u16) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定成功");
    let address = listener.local_addr().expect("地址");
    let handle = thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buffer = [0_u8; 2048];
            let _ = stream.read(&mut buffer);
            let response =
                format!("HTTP/1.1 {status} OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
            let _ = stream.write_all(response.as_bytes());
        }
    });
    (format!("127.0.0.1:{}", address.port()), handle)
}

#[test]
fn should_post_task_to_http_endpoint() {
    let (endpoint, server) = spawn_http_server(202);
    let adapter = HttpAdapter::new("hook", &format!("http://{endpoint}/tasks"))
        .expect("构造成功")
        .timeout(Duration::from_secs(5));
    adapter.call(&task("notify")).expect("2xx 应视为成功");
    server.join().expect("服务线程结束");
}

#[test]
fn should_fail_http_non_2xx() {
    let (endpoint, server) = spawn_http_server(500);
    let adapter = HttpAdapter::new("hook", &format!("http://{endpoint}/tasks"))
        .expect("构造成功")
        .timeout(Duration::from_secs(5));
    let error = adapter.call(&task("notify")).expect_err("5xx 应失败");
    assert!(error.to_string().contains("500"), "错误应含状态码: {error}");
    server.join().expect("服务线程结束");
}

#[test]
fn should_reject_non_http_url() {
    assert!(HttpAdapter::new("bad", "https://example.com/hook").is_err());
}

// ---- MCP ----

#[test]
fn should_call_mcp_tool_over_stdio() {
    let server = env!("CARGO_BIN_EXE_ah-fake-mcp");
    let adapter = McpAdapter::new("mcp", server, "agent.echo").arguments({
        let mut arguments = Value::object();
        arguments.insert("scope", Value::String("daily".to_string()));
        arguments
    });
    adapter.call(&task("agent")).expect("MCP tool 调用应成功");
}

// ---- Handler 与路由 ----

#[test]
fn should_convert_adapter_to_handler() {
    let adapter: DynAdapter = Arc::new(CommandAdapter::from_shell("ok", "exit 0"));
    let ok_handler = into_handler(Arc::clone(&adapter));
    assert!(ok_handler(&task("q")).is_ok());

    let failing: DynAdapter = Arc::new(CommandAdapter::from_shell("bad", "exit 1"));
    let bad_handler = into_handler(Arc::clone(&failing));
    let error = bad_handler(&task("q")).expect_err("失败动作应返回内核错误");
    assert!(matches!(error, agentheart_core::Error::Internal(_)));
}

#[test]
fn should_route_by_queue_and_fallback() {
    let routes = vec![(
        "build".to_string(),
        Arc::new(CommandAdapter::from_shell("build", "exit 0")) as DynAdapter,
    )];
    let fallback: DynAdapter = Arc::new(CommandAdapter::from_shell("any", "exit 0"));
    let handler = router(routes, Some(Arc::clone(&fallback)));

    assert!(handler(&task("build")).is_ok(), "命中路由应成功");
    assert!(handler(&task("other")).is_ok(), "未命中应用兜底");

    let strict = router(Vec::new(), None);
    let error = strict(&task("other")).expect_err("无路由无兜底应失败");
    assert!(matches!(error, agentheart_core::Error::Protocol(_)));
}

// ---- 适配器名称 ----

#[test]
fn should_expose_adapter_names() {
    assert_eq!(CommandAdapter::from_shell("cmd", "exit 0").name(), "cmd");
    let adapter = HttpAdapter::new("hook", "http://127.0.0.1:9/x").expect("构造成功");
    assert_eq!(adapter.name(), "hook");
    assert_eq!(McpAdapter::new("mcp", "noop", "t").name(), "mcp");
}
