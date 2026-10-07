//! 命令行适配器：把任务映射为一次 shell / 程序调用。

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use agentheart_core::Task;

use crate::{AdapterError, AdapterResult, AgentAdapter};

/// 轮询子进程退出的间隔（避免忙等）。
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// 命令行适配器。
///
/// 任务上下文会以环境变量注入子进程：
/// `AGENTHEART_TASK_ID`、`AGENTHEART_TASK_QUEUE`、`AGENTHEART_TASK_NAME`（可选）。
///
/// 说明：为规避管道背压死锁，子进程的 stdout/stderr 指向空设备（如需采集输出，
/// 宿主可自行包一层并把输出写入文件）。
#[derive(Debug, Clone)]
pub struct CommandAdapter {
    name: String,
    program: String,
    args: Vec<String>,
    cwd: Option<PathBuf>,
    envs: Vec<(String, String)>,
    timeout: Option<Duration>,
}

impl CommandAdapter {
    /// 以「程序 + 参数」构造。
    pub fn new(name: impl Into<String>, program: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            program: program.into(),
            args: Vec::new(),
            cwd: None,
            envs: Vec::new(),
            timeout: None,
        }
    }

    /// 以平台 shell 执行整条命令（Windows 用 `cmd /C`，其它平台用 `sh -c`）。
    pub fn from_shell(name: impl Into<String>, command: impl Into<String>) -> Self {
        let program = if cfg!(windows) { "cmd" } else { "sh" };
        let flag = if cfg!(windows) { "/C" } else { "-c" };
        Self::new(name, program).arg(flag).arg(command.into())
    }

    /// 追加一个参数。
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// 追加多个参数。
    pub fn args(mut self, args: impl IntoIterator<Item = String>) -> Self {
        self.args.extend(args);
        self
    }

    /// 设置工作目录。
    pub fn cwd(mut self, dir: impl Into<PathBuf>) -> Self {
        self.cwd = Some(dir.into());
        self
    }

    /// 追加环境变量。
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.envs.push((key.into(), value.into()));
        self
    }

    /// 设置执行超时（超过则强杀子进程并返回错误）。
    pub fn timeout(mut self, limit: Duration) -> Self {
        self.timeout = Some(limit);
        self
    }
}

impl AgentAdapter for CommandAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn call(&self, task: &Task) -> AdapterResult<()> {
        let mut command = Command::new(&self.program);
        command.args(&self.args);
        if let Some(cwd) = &self.cwd {
            command.current_dir(cwd);
        }
        for (key, value) in &self.envs {
            command.env(key, value);
        }
        command.env("AGENTHEART_TASK_ID", task.id.as_str());
        command.env("AGENTHEART_TASK_QUEUE", &task.queue);
        if let Some(name) = &task.name {
            command.env("AGENTHEART_TASK_NAME", name);
        }
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let mut child = command.spawn()?;
        let started = Instant::now();
        loop {
            if let Some(status) = child.try_wait()? {
                return if status.success() {
                    Ok(())
                } else {
                    Err(AdapterError::new(format!("命令退出码非零: {status}")))
                };
            }
            if let Some(limit) = self.timeout {
                if started.elapsed() >= limit {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(AdapterError::new(format!(
                        "命令超时（{}ms）",
                        limit.as_millis()
                    )));
                }
            }
            thread::sleep(POLL_INTERVAL);
        }
    }
}
