//! 持久化：追加写 WAL 与原子写入。

mod wal;

pub use wal::{Wal, write_atomic};
