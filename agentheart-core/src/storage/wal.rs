//! 持久化：追加写 WAL（零依赖）。
//!
//! 采用「追加写 + 启动回放」的经典 WAL 模式；记录以自研 JSON 单行存储，
//! 便于人工排查与跨语言处理。原子快照写入使用「临时文件 + 改名」。

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use crate::error::Result;
use crate::json::{self, Value};

/// 追加写日志。
#[derive(Debug)]
pub struct Wal {
    path: PathBuf,
    file: Mutex<File>,
    sync: bool,
}

impl Wal {
    /// 打开（不存在则创建）指定路径的 WAL。
    ///
    /// # Errors
    /// 文件无法打开时返回 [`Error::Io`]。
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        Ok(Self {
            path,
            file: Mutex::new(file),
            sync: false,
        })
    }

    /// 设置是否在每次追加后 `fsync`（更安全但更慢）。
    pub fn with_sync(mut self, sync: bool) -> Self {
        self.sync = sync;
        self
    }

    /// 日志文件路径。
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 追加一条记录（单行 JSON）。
    ///
    /// # Errors
    /// 序列化或写入失败时返回错误。
    pub fn append(&self, record: &Value) -> Result<()> {
        let line = record.to_json_string();
        let mut file = self.file.lock().unwrap_or_else(PoisonError::into_inner);
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
        if self.sync {
            file.sync_data()?;
        }
        Ok(())
    }

    /// 回放全部记录。
    ///
    /// # Errors
    /// 读取失败或存在无法解析的行时返回错误。
    pub fn replay(&self) -> Result<Vec<Value>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut records = Vec::new();
        for line in reader.lines() {
            let line = line?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            records.push(json::parse(trimmed)?);
        }
        Ok(records)
    }
}

/// 以原子方式写入文件（临时文件 + 改名）。
///
/// # Errors
/// 写入或改名失败时返回 [`Error::Io`]。
pub fn write_atomic(path: impl AsRef<Path>, bytes: &[u8]) -> Result<()> {
    let path = path.as_ref();
    let temp = path.with_extension("tmp");
    {
        let mut file = File::create(&temp)?;
        file.write_all(bytes)?;
        file.sync_data()?;
    }
    fs::rename(&temp, path)?;
    Ok(())
}
