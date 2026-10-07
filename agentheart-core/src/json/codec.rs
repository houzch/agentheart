// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! JSON 编解码实现（RFC 8259 子集，零第三方依赖）。
//!
//! - 解析：递归下降，带**深度上限**，防御恶意嵌套；严格校验转义、`\u` 代理对与 UTF-8；
//! - 序列化：对象保持键的**插入顺序**；
//! - 数字统一以 `f64` 表示（非有限值序列化为 `null`）。

use crate::error::{Error, Result};

/// 解析深度上限。
const MAX_DEPTH: usize = 128;

/// JSON 值。
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// `null`。
    Null,
    /// 布尔。
    Bool(bool),
    /// 数字（统一以 `f64` 表示）。
    Number(f64),
    /// 字符串。
    String(String),
    /// 数组。
    Array(Vec<Value>),
    /// 对象（保持插入顺序）。
    Object(Vec<(String, Value)>),
}

impl Value {
    /// 空对象。
    pub const fn object() -> Self {
        Value::Object(Vec::new())
    }

    /// 空数组。
    pub const fn array() -> Self {
        Value::Array(Vec::new())
    }

    /// 是否为 `null`。
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// 取字符串视图。
    pub fn as_str(&self) -> Option<&str> {
        if let Value::String(value) = self {
            Some(value)
        } else {
            None
        }
    }

    /// 取数字。
    pub fn as_f64(&self) -> Option<f64> {
        if let Value::Number(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    /// 取布尔。
    pub fn as_bool(&self) -> Option<bool> {
        if let Value::Bool(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    /// 取数组切片。
    pub fn as_array(&self) -> Option<&[Value]> {
        if let Value::Array(items) = self {
            Some(items)
        } else {
            None
        }
    }

    /// 取对象切片。
    pub fn as_object(&self) -> Option<&[(String, Value)]> {
        if let Value::Object(entries) = self {
            Some(entries)
        } else {
            None
        }
    }

    /// 按 key 取对象成员。
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.as_object()?
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value)
    }

    /// 向对象插入键值；非对象时返回 `false`。
    pub fn insert(&mut self, key: impl Into<String>, value: Value) -> bool {
        if let Value::Object(entries) = self {
            let key = key.into();
            if let Some(slot) = entries.iter_mut().find(|(name, _)| *name == key) {
                slot.1 = value;
            } else {
                entries.push((key, value));
            }
            true
        } else {
            false
        }
    }

    /// 序列化为 JSON 文本。
    pub fn to_json_string(&self) -> String {
        let mut out = String::new();
        self.write_json(&mut out);
        out
    }

    fn write_json(&self, out: &mut String) {
        match self {
            Value::Null => out.push_str("null"),
            Value::Bool(true) => out.push_str("true"),
            Value::Bool(false) => out.push_str("false"),
            Value::Number(number) => {
                if number.is_finite() {
                    out.push_str(&number.to_string());
                } else {
                    out.push_str("null");
                }
            }
            Value::String(text) => write_escaped(out, text),
            Value::Array(items) => {
                out.push('[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    item.write_json(out);
                }
                out.push(']');
            }
            Value::Object(entries) => {
                out.push('{');
                for (index, (key, value)) in entries.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    write_escaped(out, key);
                    out.push(':');
                    value.write_json(out);
                }
                out.push('}');
            }
        }
    }
}

impl core::fmt::Display for Value {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.to_json_string())
    }
}

fn write_escaped(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            control if (control as u32) < 0x20 => {
                out.push_str("\\u");
                out.push_str(&format!("{:04x}", control as u32));
            }
            other => out.push(other),
        }
    }
    out.push('"');
}

/// 解析 JSON 文本。
///
/// # Errors
/// 当输入不是合法 JSON 时返回 [`Error::Json`]。
pub fn parse(input: &str) -> Result<Value> {
    parse_bytes(input.as_bytes())
}

/// 解析 JSON 字节切片（须为 UTF-8）。
///
/// # Errors
/// 当输入不是合法 JSON 时返回 [`Error::Json`]。
pub fn parse_bytes(input: &[u8]) -> Result<Value> {
    Parser::new(input).parse()
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
    depth: usize,
}

impl<'a> Parser<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            pos: 0,
            depth: 0,
        }
    }

    fn parse(&mut self) -> Result<Value> {
        self.skip_whitespace();
        let value = self.parse_value()?;
        self.skip_whitespace();
        if self.pos != self.bytes.len() {
            return Err(self.error("trailing characters"));
        }
        Ok(value)
    }

    fn parse_value(&mut self) -> Result<Value> {
        if self.depth > MAX_DEPTH {
            return Err(self.error("max nesting depth exceeded"));
        }
        match self.peek() {
            Some(b'n') => {
                self.expect_literal(b"null")?;
                Ok(Value::Null)
            }
            Some(b't') => {
                self.expect_literal(b"true")?;
                Ok(Value::Bool(true))
            }
            Some(b'f') => {
                self.expect_literal(b"false")?;
                Ok(Value::Bool(false))
            }
            Some(b'"') => Ok(Value::String(self.parse_string()?)),
            Some(b'[') => self.parse_array(),
            Some(b'{') => self.parse_object(),
            Some(b'-' | b'0'..=b'9') => self.parse_number(),
            Some(_) => Err(self.error("unexpected character")),
            None => Err(self.error("unexpected end of input")),
        }
    }

    fn parse_array(&mut self) -> Result<Value> {
        self.next_byte(); // '['
        self.depth += 1;
        let mut items = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b']') {
            self.next_byte();
            self.depth -= 1;
            return Ok(Value::Array(items));
        }
        loop {
            self.skip_whitespace();
            items.push(self.parse_value()?);
            self.skip_whitespace();
            match self.next_byte() {
                Some(b',') => {}
                Some(b']') => break,
                _ => return Err(self.error("expected ',' or ']'")),
            }
        }
        self.depth -= 1;
        Ok(Value::Array(items))
    }

    fn parse_object(&mut self) -> Result<Value> {
        self.next_byte(); // '{'
        self.depth += 1;
        let mut entries: Vec<(String, Value)> = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.next_byte();
            self.depth -= 1;
            return Ok(Value::Object(entries));
        }
        loop {
            self.skip_whitespace();
            if self.peek() != Some(b'"') {
                return Err(self.error("expected object key"));
            }
            let key = self.parse_string()?;
            self.skip_whitespace();
            if self.next_byte() != Some(b':') {
                return Err(self.error("expected ':'"));
            }
            self.skip_whitespace();
            let value = self.parse_value()?;
            entries.push((key, value));
            self.skip_whitespace();
            match self.next_byte() {
                Some(b',') => {}
                Some(b'}') => break,
                _ => return Err(self.error("expected ',' or '}'")),
            }
        }
        self.depth -= 1;
        Ok(Value::Object(entries))
    }

    fn parse_number(&mut self) -> Result<Value> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.next_byte();
        }
        match self.peek() {
            Some(b'0') => {
                self.next_byte();
            }
            Some(b'1'..=b'9') => {
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.next_byte();
                }
            }
            _ => return Err(self.error("invalid number")),
        }
        if self.peek() == Some(b'.') {
            self.next_byte();
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(self.error("invalid fraction"));
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.next_byte();
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.next_byte();
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.next_byte();
            }
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(self.error("invalid exponent"));
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.next_byte();
            }
        }
        let text = std::str::from_utf8(&self.bytes[start..self.pos])
            .map_err(|_| self.error("invalid number"))?;
        text.parse::<f64>()
            .map(Value::Number)
            .map_err(|_| self.error("number out of range"))
    }

    fn parse_string(&mut self) -> Result<String> {
        self.next_byte(); // 起始引号
        let mut buffer: Vec<u8> = Vec::new();
        loop {
            let Some(byte) = self.next_byte() else {
                return Err(self.error("unterminated string"));
            };
            match byte {
                b'"' => {
                    return String::from_utf8(buffer).map_err(|_| self.error("invalid utf-8"));
                }
                b'\\' => self.parse_escape(&mut buffer)?,
                control if control < 0x20 => {
                    return Err(self.error("control character in string"));
                }
                other => buffer.push(other),
            }
        }
    }

    fn parse_escape(&mut self, buffer: &mut Vec<u8>) -> Result<()> {
        let Some(escaped) = self.next_byte() else {
            return Err(self.error("unterminated escape"));
        };
        match escaped {
            b'"' => buffer.push(b'"'),
            b'\\' => buffer.push(b'\\'),
            b'/' => buffer.push(b'/'),
            b'b' => buffer.push(0x08),
            b'f' => buffer.push(0x0c),
            b'n' => buffer.push(b'\n'),
            b'r' => buffer.push(b'\r'),
            b't' => buffer.push(b'\t'),
            b'u' => {
                let decoded = self.parse_unicode_escape()?;
                let mut encoded = [0_u8; 4];
                buffer.extend_from_slice(decoded.encode_utf8(&mut encoded).as_bytes());
            }
            _ => return Err(self.error("invalid escape sequence")),
        }
        Ok(())
    }

    fn parse_unicode_escape(&mut self) -> Result<char> {
        let high = self.parse_hex4()?;
        let code_point = if (0xD800..=0xDBFF).contains(&high) {
            if self.peek() == Some(b'\\') && self.bytes.get(self.pos + 1) == Some(&b'u') {
                self.next_byte();
                self.next_byte();
                let low = self.parse_hex4()?;
                if !(0xDC00..=0xDFFF).contains(&low) {
                    return Err(self.error("invalid low surrogate"));
                }
                let combined =
                    0x1_0000 + ((u32::from(high) - 0xD800) << 10) + (u32::from(low) - 0xDC00);
                char::from_u32(combined).ok_or_else(|| self.error("invalid code point"))?
            } else {
                return Err(self.error("lone high surrogate"));
            }
        } else if (0xDC00..=0xDFFF).contains(&high) {
            return Err(self.error("lone low surrogate"));
        } else {
            char::from_u32(u32::from(high)).ok_or_else(|| self.error("invalid code point"))?
        };
        Ok(code_point)
    }

    fn parse_hex4(&mut self) -> Result<u16> {
        let mut value: u16 = 0;
        for _ in 0..4 {
            let Some(byte) = self.next_byte() else {
                return Err(self.error("incomplete unicode escape"));
            };
            let digit = match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                b'A'..=b'F' => byte - b'A' + 10,
                _ => return Err(self.error("invalid hex digit")),
            };
            value = (value << 4) | u16::from(digit);
        }
        Ok(value)
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn next_byte(&mut self) -> Option<u8> {
        let byte = self.bytes.get(self.pos).copied();
        if byte.is_some() {
            self.pos += 1;
        }
        byte
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn expect_literal(&mut self, literal: &[u8]) -> Result<()> {
        let end = self.pos + literal.len();
        if end <= self.bytes.len() && &self.bytes[self.pos..end] == literal {
            self.pos = end;
            Ok(())
        } else {
            Err(self.error("invalid literal"))
        }
    }

    fn error(&self, message: &str) -> Error {
        Error::Json {
            message: format!("{message} (at byte {})", self.pos),
        }
    }
}
