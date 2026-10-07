//! 内核接口协议：帧编解码（16 字节头 + UTF-8 JSON 载荷）。
//!
//! 帧头布局（小端）：
//!
//! | 偏移 | 字段        | 长度 |
//! |------|-------------|------|
//! | 0    | Magic       | 4B   |
//! | 4    | Ver         | 1B   |
//! | 5    | Type        | 1B   |
//! | 6    | Flags       | 2B   |
//! | 8    | RequestID   | 4B   |
//! | 12   | PayloadLen  | 4B   |
//! | 16   | Payload     | 变长 |

use std::io::{Read, Write};

use crate::error::{Error, Result};

/// 协议魔数（`"AH"` + 版本标识）。
pub const MAGIC: u32 = 0x4148_0001;
/// 帧头长度。
pub const HEADER_LEN: usize = 16;
/// 载荷上限（1 MiB）。
pub const MAX_PAYLOAD: usize = 1 << 20;
/// 协议主版本。
pub const PROTOCOL_VERSION: u8 = 1;

/// 帧标志位。
pub mod flags {
    /// 请求。
    pub const REQUEST: u16 = 0x1;
    /// 响应。
    pub const RESPONSE: u16 = 0x2;
    /// 事件推送。
    pub const EVENT: u16 = 0x4;
    /// 错误响应。
    pub const ERROR: u16 = 0x8;
}

/// 一帧。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// 协议版本。
    pub version: u8,
    /// 消息类型（保留字段）。
    pub msg_type: u8,
    /// 标志位。
    pub flags: u16,
    /// 请求-响应关联 ID；事件帧为 0。
    pub request_id: u32,
    /// 载荷（UTF-8 JSON）。
    pub payload: Vec<u8>,
}

impl Frame {
    /// 构造请求帧。
    pub fn request(request_id: u32, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            msg_type: 0,
            flags: flags::REQUEST,
            request_id,
            payload: payload.into(),
        }
    }

    /// 构造响应帧。
    pub fn response(request_id: u32, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            msg_type: 0,
            flags: flags::RESPONSE,
            request_id,
            payload: payload.into(),
        }
    }

    /// 构造错误响应帧。
    pub fn error(request_id: u32, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            msg_type: 0,
            flags: flags::ERROR,
            request_id,
            payload: payload.into(),
        }
    }

    /// 构造事件帧。
    pub fn event(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            msg_type: 0,
            flags: flags::EVENT,
            request_id: 0,
            payload: payload.into(),
        }
    }

    /// 编码为字节。
    ///
    /// # Errors
    /// 载荷超过 [`MAX_PAYLOAD`] 时返回 [`Error::Protocol`]。
    pub fn encode(&self) -> Result<Vec<u8>> {
        if self.payload.len() > MAX_PAYLOAD {
            return Err(Error::Protocol("frame payload too large"));
        }
        let mut out = Vec::with_capacity(HEADER_LEN + self.payload.len());
        out.extend_from_slice(&MAGIC.to_le_bytes());
        out.push(self.version);
        out.push(self.msg_type);
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.extend_from_slice(&self.request_id.to_le_bytes());
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.payload);
        Ok(out)
    }

    /// 从字节缓冲解码，返回帧与消耗的字节数。
    ///
    /// # Errors
    /// 帧头/载荷不完整或魔数非法时返回 [`Error::Protocol`]。
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize)> {
        if bytes.len() < HEADER_LEN {
            return Err(Error::Protocol("frame header truncated"));
        }
        let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if magic != MAGIC {
            return Err(Error::Protocol("bad frame magic"));
        }
        let payload_len = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]) as usize;
        if payload_len > MAX_PAYLOAD {
            return Err(Error::Protocol("frame payload too large"));
        }
        let end = HEADER_LEN + payload_len;
        if bytes.len() < end {
            return Err(Error::Protocol("frame payload truncated"));
        }
        Ok((
            Self {
                version: bytes[4],
                msg_type: bytes[5],
                flags: u16::from_le_bytes([bytes[6], bytes[7]]),
                request_id: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
                payload: bytes[HEADER_LEN..end].to_vec(),
            },
            end,
        ))
    }
}

/// 写出一个帧并刷新。
///
/// # Errors
/// 编码或写入失败时返回错误。
pub fn write_frame<W: Write>(writer: &mut W, frame: &Frame) -> Result<()> {
    writer.write_all(&frame.encode()?)?;
    writer.flush()?;
    Ok(())
}

/// 读入一个帧。
///
/// # Errors
/// 读取失败或帧非法时返回错误。
pub fn read_frame<R: Read>(reader: &mut R) -> Result<Frame> {
    let mut header = [0_u8; HEADER_LEN];
    reader.read_exact(&mut header)?;
    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    if magic != MAGIC {
        return Err(Error::Protocol("bad frame magic"));
    }
    let payload_len = u32::from_le_bytes([header[12], header[13], header[14], header[15]]) as usize;
    if payload_len > MAX_PAYLOAD {
        return Err(Error::Protocol("frame payload too large"));
    }
    let mut payload = vec![0_u8; payload_len];
    reader.read_exact(&mut payload)?;
    Ok(Frame {
        version: header[4],
        msg_type: header[5],
        flags: u16::from_le_bytes([header[6], header[7]]),
        request_id: u32::from_le_bytes([header[8], header[9], header[10], header[11]]),
        payload,
    })
}
