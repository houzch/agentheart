// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! MQTT 3.1.1 报文编解码（子集，零依赖）。
//!
//! 固定头：`byte0 = type<<4 | flags`，随后为**变长剩余长度**（7 位一组，最多 4 字节）。

use std::io::Read;

use crate::error::{Error, Result};

/// 报文类型常量。
pub(crate) mod kind {
    /// CONNECT。
    pub(crate) const CONNECT: u8 = 1;
    /// CONNACK。
    pub(crate) const CONNACK: u8 = 2;
    /// PUBLISH。
    pub(crate) const PUBLISH: u8 = 3;
    /// PUBACK。
    pub(crate) const PUBACK: u8 = 4;
    /// SUBSCRIBE。
    pub(crate) const SUBSCRIBE: u8 = 8;
    /// SUBACK。
    pub(crate) const SUBACK: u8 = 9;
    /// UNSUBSCRIBE。
    pub(crate) const UNSUBSCRIBE: u8 = 10;
    /// UNSUBACK。
    pub(crate) const UNSUBACK: u8 = 11;
    /// PINGREQ。
    pub(crate) const PINGREQ: u8 = 12;
    /// PINGRESP。
    pub(crate) const PINGRESP: u8 = 13;
    /// DISCONNECT。
    pub(crate) const DISCONNECT: u8 = 14;
}

/// CONNACK 返回码。
pub(crate) mod connack_code {
    /// 接受。
    pub(crate) const ACCEPTED: u8 = 0;
    /// 用户名或密码错误。
    pub(crate) const BAD_CREDENTIALS: u8 = 4;
    /// 协议版本不支持。
    pub(crate) const BAD_PROTOCOL: u8 = 1;
}

/// 单个报文的最大长度（1 MiB）。
const MAX_PACKET: usize = 1 << 20;

/// 已解析的报文（固定头 + 载荷）。
#[derive(Debug, Clone)]
pub(crate) struct Packet {
    pub(crate) packet_type: u8,
    pub(crate) flags: u8,
    pub(crate) payload: Vec<u8>,
}

/// 读入一个完整报文。
pub(crate) fn read_packet<R: Read>(reader: &mut R) -> Result<Packet> {
    let mut first = [0_u8; 1];
    reader.read_exact(&mut first)?;
    let remaining = read_varint(reader)?;
    if remaining > MAX_PACKET {
        return Err(Error::Protocol("mqtt packet too large"));
    }
    let mut payload = vec![0_u8; remaining];
    reader.read_exact(&mut payload)?;
    Ok(Packet {
        packet_type: first[0] >> 4,
        flags: first[0] & 0x0F,
        payload,
    })
}

/// 编码变长剩余长度。
pub(crate) fn write_varint(out: &mut Vec<u8>, mut value: usize) {
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// 解析变长剩余长度。
pub(crate) fn read_varint<R: Read>(reader: &mut R) -> Result<usize> {
    let mut value = 0_usize;
    let mut multiplier = 1_usize;
    for _ in 0..4 {
        let mut byte = [0_u8; 1];
        reader.read_exact(&mut byte)?;
        value += usize::from(byte[0] & 0x7F) * multiplier;
        if byte[0] & 0x80 == 0 {
            return Ok(value);
        }
        multiplier *= 128;
    }
    Err(Error::Protocol("malformed mqtt remaining length"))
}

/// 组包（固定头 + 变长长度 + 载荷）。
pub(crate) fn encode(packet_type: u8, flags: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push((packet_type << 4) | (flags & 0x0F));
    write_varint(&mut out, body.len());
    out.extend_from_slice(body);
    out
}

fn push_string(out: &mut Vec<u8>, text: &str) {
    let len = u16::try_from(text.len()).unwrap_or(u16::MAX);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&text.as_bytes()[..usize::from(len)]);
}

/// CONNACK。
pub(crate) fn connack(session_present: bool, code: u8) -> Vec<u8> {
    let body = [u8::from(session_present), code];
    encode(kind::CONNACK, 0, &body)
}

/// PUBLISH（下行，QoS 0）。
pub(crate) fn publish_qos0(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(topic.len() + payload.len() + 2);
    push_string(&mut body, topic);
    body.extend_from_slice(payload);
    encode(kind::PUBLISH, 0, &body)
}

/// PUBLISH（下行，QoS 1；`dup` 为重传标志）。
pub(crate) fn publish_qos1(topic: &str, payload: &[u8], packet_id: u16, dup: bool) -> Vec<u8> {
    let mut body = Vec::with_capacity(topic.len() + payload.len() + 4);
    push_string(&mut body, topic);
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.extend_from_slice(payload);
    let flags = if dup { 0x08 | 0x02 } else { 0x02 };
    encode(kind::PUBLISH, flags, &body)
}

/// PUBACK。
pub(crate) fn puback(packet_id: u16) -> Vec<u8> {
    encode(kind::PUBACK, 0, &packet_id.to_be_bytes())
}

/// SUBACK。
pub(crate) fn suback(packet_id: u16, codes: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(3 + codes.len());
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.extend_from_slice(codes);
    encode(kind::SUBACK, 0, &body)
}

/// UNSUBACK。
pub(crate) fn unsuback(packet_id: u16) -> Vec<u8> {
    encode(kind::UNSUBACK, 0, &packet_id.to_be_bytes())
}

/// PINGRESP。
pub(crate) fn pingresp() -> Vec<u8> {
    encode(kind::PINGRESP, 0, &[])
}

/// CONNECT 中的遗嘱（Will）。
#[derive(Debug, Clone)]
pub(crate) struct Will {
    pub(crate) topic: String,
    pub(crate) payload: Vec<u8>,
    /// 子集实现仅支持到 QoS 1。
    pub(crate) qos: u8,
    pub(crate) retain: bool,
}

/// CONNECT 解析结果。
#[derive(Debug, Clone)]
pub(crate) struct ConnectData {
    pub(crate) client_id: String,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) will: Option<Will>,
}

/// 解析 CONNECT 载荷。
pub(crate) fn parse_connect(payload: &[u8]) -> Result<ConnectData> {
    let mut reader = Reader::new(payload);
    if reader.string()? != "MQTT" {
        return Err(Error::Protocol("unsupported mqtt protocol name"));
    }
    if reader.u8()? != 4 {
        return Err(Error::Protocol("unsupported mqtt protocol level"));
    }
    let flags = reader.u8()?;
    let _keep_alive = reader.u16()?;
    let client_id = reader.string()?;
    let will = if flags & 0x04 != 0 {
        let topic = reader.string()?;
        let will_len = usize::from(reader.u16()?);
        Some(Will {
            topic,
            payload: reader.bytes(will_len)?.to_vec(),
            qos: ((flags >> 3) & 0x03).min(1),
            retain: flags & 0x20 != 0,
        })
    } else {
        None
    };
    let username = if flags & 0x80 != 0 {
        Some(reader.string()?)
    } else {
        None
    };
    let password = if flags & 0x40 != 0 {
        Some(reader.string()?)
    } else {
        None
    };
    Ok(ConnectData {
        client_id,
        username,
        password,
        will,
    })
}

/// 解析 SUBSCRIBE（返回包 ID 与 `(过滤器, 请求 QoS)` 列表）。
pub(crate) fn parse_subscribe(payload: &[u8]) -> Result<(u16, Vec<(String, u8)>)> {
    let mut reader = Reader::new(payload);
    let packet_id = reader.u16()?;
    let mut filters = Vec::new();
    while reader.remaining() > 0 {
        let filter = reader.string()?;
        let requested = reader.u8()? & 0x03;
        filters.push((filter, requested));
    }
    if filters.is_empty() {
        return Err(Error::Protocol("empty mqtt subscribe"));
    }
    Ok((packet_id, filters))
}

/// 解析 UNSUBSCRIBE（返回包 ID 与过滤器列表）。
pub(crate) fn parse_unsubscribe(payload: &[u8]) -> Result<(u16, Vec<String>)> {
    let mut reader = Reader::new(payload);
    let packet_id = reader.u16()?;
    let mut filters = Vec::new();
    while reader.remaining() > 0 {
        filters.push(reader.string()?);
    }
    Ok((packet_id, filters))
}

/// PUBLISH 解析结果。
#[derive(Debug, Clone)]
pub(crate) struct PublishData {
    pub(crate) topic: String,
    pub(crate) packet_id: Option<u16>,
    pub(crate) payload: Vec<u8>,
}

/// 解析 PUBLISH。
pub(crate) fn parse_publish(flags: u8, payload: &[u8]) -> Result<PublishData> {
    let qos = (flags >> 1) & 0x03;
    if qos > 1 {
        return Err(Error::Protocol("mqtt qos 2 not supported"));
    }
    let mut reader = Reader::new(payload);
    let topic = reader.string()?;
    let packet_id = if qos > 0 { Some(reader.u16()?) } else { None };
    Ok(PublishData {
        topic,
        packet_id,
        payload: reader.rest().to_vec(),
    })
}

/// MQTT 主题过滤器匹配（支持 `+` / `#`，并遵循 `$` 前缀规则）。
pub(crate) fn topic_matches(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && (filter.starts_with('+') || filter.starts_with('#')) {
        return false;
    }
    let mut filter_levels = filter.split('/');
    let mut topic_levels = topic.split('/');
    loop {
        match (filter_levels.next(), topic_levels.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => {}
            (Some(left), Some(right)) if left == right => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

/// 载荷读取器。
struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn u8(&mut self) -> Result<u8> {
        if self.remaining() < 1 {
            return Err(Error::Protocol("mqtt payload truncated"));
        }
        let value = self.bytes[self.pos];
        self.pos += 1;
        Ok(value)
    }

    fn u16(&mut self) -> Result<u16> {
        let bytes = self.bytes(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.remaining() < len {
            return Err(Error::Protocol("mqtt payload truncated"));
        }
        let slice = &self.bytes[self.pos..self.pos + len];
        self.pos += len;
        Ok(slice)
    }

    fn string(&mut self) -> Result<String> {
        let len = usize::from(self.u16()?);
        let bytes = self.bytes(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| Error::Protocol("mqtt string not utf-8"))
    }

    fn rest(&mut self) -> &'a [u8] {
        let slice = &self.bytes[self.pos..];
        self.pos = self.bytes.len();
        slice
    }
}
