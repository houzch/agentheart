//! 基于文件独占创建的进程锁：用于集群选主 / 单例保护（零依赖）。

use std::fs::{self, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// 文件锁：持有期间占据一个独占文件；进程退出（Drop）时释放。
#[derive(Debug)]
pub struct FileLock {
    path: PathBuf,
    held: bool,
}

impl FileLock {
    /// 尝试获取文件锁。
    ///
    /// 返回 `Ok(Some(lock))` 表示获取成功；`Ok(None)` 表示已被其它实例持有。
    ///
    /// # Errors
    /// 文件创建失败（非"已存在"）时返回 [`Error::Io`]。
    pub fn try_acquire(path: impl Into<PathBuf>) -> Result<Option<Self>> {
        let path = path.into();
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => {
                drop(file);
                Ok(Some(Self { path, held: true }))
            }
            Err(err) if err.kind() == ErrorKind::AlreadyExists => Ok(None),
            Err(err) => Err(Error::Io(err)),
        }
    }

    /// 当前是否持有锁。
    pub fn is_held(&self) -> bool {
        self.held
    }

    /// 锁文件路径。
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        if self.held {
            let _ = fs::remove_file(&self.path);
        }
    }
}
