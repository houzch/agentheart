//! 游标分页：基于排序键的稳定分页（零依赖）。
//!
//! 游标为**不透明字符串**：`hex("<sortValue>:<id>")`，客户端不得解析。
//! 排序键为 `(sortValue, id)`，`desc` 时整体降序、`asc` 时整体升序，
//! 因此翻页在数据变动时仍保持稳定（非 offset）。

use crate::json::Value;

/// 分页参数。
#[derive(Debug, Clone)]
pub(crate) struct Page {
    /// 每页条数（1..=500）。
    pub(crate) limit: usize,
    /// 是否降序。
    pub(crate) desc: bool,
    /// 排序字段。
    pub(crate) sort_by: String,
    /// 游标：`(排序值, ID)`。
    pub(crate) cursor: Option<(u64, String)>,
}

impl Page {
    /// 从请求的 `page` 对象解析（缺省 `limit=50`、`order=desc`、排序字段取 `default_sort`）。
    pub(crate) fn from_request(request: &Value, default_sort: &str) -> Self {
        let page = request.get("page");
        let limit = page
            .and_then(|value| value.get("limit"))
            .and_then(Value::as_f64)
            .unwrap_or(50.0)
            .clamp(1.0, 500.0) as usize;
        let desc = !matches!(
            page.and_then(|value| value.get("order"))
                .and_then(Value::as_str),
            Some("asc")
        );
        let sort_by = page
            .and_then(|value| value.get("sortBy"))
            .and_then(Value::as_str)
            .unwrap_or(default_sort)
            .to_string();
        let cursor = page
            .and_then(|value| value.get("cursor"))
            .and_then(Value::as_str)
            .and_then(parse_cursor);
        Self {
            limit,
            desc,
            sort_by,
            cursor,
        }
    }
}

/// 对 `(排序值, ID, 元素)` 列表分页，返回当页元素与下一页游标。
pub(crate) fn page_slice<T: Clone>(
    mut rows: Vec<(u64, String, T)>,
    page: &Page,
) -> (Vec<T>, Option<String>) {
    rows.sort_by(|left, right| {
        let ordering = left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1));
        if page.desc {
            ordering.reverse()
        } else {
            ordering
        }
    });

    let start = match &page.cursor {
        None => 0,
        Some((cursor_value, cursor_id)) => rows
            .iter()
            .position(|(value, id, _)| {
                let key = (*value, id.as_str());
                let cursor = (*cursor_value, cursor_id.as_str());
                if page.desc {
                    key < cursor
                } else {
                    key > cursor
                }
            })
            .unwrap_or(rows.len()),
    };
    let end = start.saturating_add(page.limit).min(rows.len());
    let items: Vec<T> = rows
        .get(start..end)
        .map(|slice| slice.iter().map(|(_, _, item)| item.clone()).collect())
        .unwrap_or_default();
    let next_cursor = if end < rows.len() && end > start {
        rows.get(end - 1)
            .map(|(value, id, _)| encode_cursor(*value, id))
    } else {
        None
    };
    (items, next_cursor)
}

/// 编码游标（不透明十六进制）。
pub(crate) fn encode_cursor(value: u64, id: &str) -> String {
    hex_encode(format!("{value}:{id}").as_bytes())
}

/// 解析游标。
pub(crate) fn parse_cursor(text: &str) -> Option<(u64, String)> {
    let decoded = String::from_utf8(hex_decode(text)?).ok()?;
    let (value, id) = decoded.split_once(':')?;
    Some((value.parse::<u64>().ok()?, id.to_string()))
}

fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    out
}

fn hex_decode(text: &str) -> Option<Vec<u8>> {
    let bytes = text.as_bytes();
    if bytes.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let high = hex_digit(pair[0])?;
        let low = hex_digit(pair[1])?;
        out.push((high << 4) | low);
    }
    Some(out)
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
