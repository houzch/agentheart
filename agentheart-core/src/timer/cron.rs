// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

//! Cron 表达式解析与下次触发时间计算（UTC，零依赖）。
//!
//! 支持 5 字段（`分 时 日 月 周`）与 6 字段（`秒 分 时 日 月 周`）。
//! 每个字段支持：`*`、`?`（等同 `*`）、单值、区间 `a-b`、步长 `a-b/s`、`*/s`、列表 `a,b,c`。

use crate::error::{Error, Result};

/// 一天的最大搜索天数（约 8 年），用于避免无解时无限循环。
const MAX_DAYS: i64 = 366 * 8;

/// 解析后的 Cron 调度。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cron {
    seconds: u64,
    minutes: u64,
    hours: u64,
    days_of_month: u64,
    months: u64,
    days_of_week: u64,
    dom_restricted: bool,
    dow_restricted: bool,
    /// 解析时的原始表达式（供协议回显，保持用户输入的等价语义）。
    expression: String,
}

impl Cron {
    /// 解析 Cron 表达式。
    ///
    /// # Errors
    /// 字段数量不为 5/6，或任一字段非法时返回 [`Error::Protocol`]。
    pub fn parse(expression: &str) -> Result<Self> {
        let fields: Vec<&str> = expression.split_whitespace().collect();
        let (seconds, minutes, hours, dom, month, dow) = match fields.as_slice() {
            [minute, hour, dom, month, dow] => (
                // 5 字段：秒固定为 0
                1_u64,
                parse_field(minute, 0, 59)?,
                parse_field(hour, 0, 23)?,
                parse_field(dom, 1, 31)?,
                parse_field(month, 1, 12)?,
                normalize_weekday(parse_field(dow, 0, 7)?),
            ),
            [second, minute, hour, dom, month, dow] => (
                parse_field(second, 0, 59)?,
                parse_field(minute, 0, 59)?,
                parse_field(hour, 0, 23)?,
                parse_field(dom, 1, 31)?,
                parse_field(month, 1, 12)?,
                normalize_weekday(parse_field(dow, 0, 7)?),
            ),
            _ => return Err(Error::Protocol("cron requires 5 or 6 fields")),
        };
        Ok(Self {
            seconds,
            minutes,
            hours,
            days_of_month: dom,
            months: month,
            days_of_week: dow,
            dom_restricted: is_restricted(fields[if fields.len() == 5 { 2 } else { 3 }]),
            dow_restricted: is_restricted(fields[fields.len() - 1]),
            expression: expression.to_string(),
        })
    }

    /// 解析时的原始 Cron 表达式。
    pub fn as_str(&self) -> &str {
        &self.expression
    }

    /// 计算严格晚于 `after_ms` 的下一个触发时间（Unix 毫秒）；无解返回 `None`。
    pub fn next_after(&self, after_ms: u64) -> Option<u64> {
        let start_ms = after_ms.saturating_add(1000);
        let start_days = (start_ms / 86_400_000) as i64;
        let start_second = ((start_ms % 86_400_000) / 1000) as u32;

        for offset in 0..=MAX_DAYS {
            let days = start_days + offset;
            let (_year, month, day) = civil_from_days(days);
            if self.months & (1_u64 << month) == 0 {
                continue;
            }
            if !self.day_matches(days, day) {
                continue;
            }
            let lower = if offset == 0 { start_second } else { 0 };
            if let Some(second_of_day) = self.first_match_in_day(lower) {
                return Some((days as u64) * 86_400_000 + u64::from(second_of_day) * 1000);
            }
        }
        None
    }

    fn day_matches(&self, days: i64, day_of_month: u32) -> bool {
        let dom_ok = self.days_of_month & (1_u64 << day_of_month) != 0;
        let dow_ok = self.days_of_week & (1_u64 << weekday_from_days(days)) != 0;
        match (self.dom_restricted, self.dow_restricted) {
            (true, true) => dom_ok || dow_ok,
            (true, false) => dom_ok,
            (false, true) => dow_ok,
            (false, false) => true,
        }
    }

    fn first_match_in_day(&self, lower: u32) -> Option<u32> {
        for hour in bit_values(self.hours, 24) {
            let hour_base = hour * 3600;
            if hour_base + 3599 < lower {
                continue;
            }
            for minute in bit_values(self.minutes, 60) {
                let minute_base = hour_base + minute * 60;
                if minute_base + 59 < lower {
                    continue;
                }
                for second in bit_values(self.seconds, 60) {
                    let at = minute_base + second;
                    if at >= lower {
                        return Some(at);
                    }
                }
            }
        }
        None
    }
}

/// 构造指定 UTC 日期时间的 Unix 毫秒（供测试与调试使用）。
///
/// `month` 取 1..=12，`day` 取 1..=31，`hour`/`minute`/`second` 取常规范围。
pub fn utc_ms(year: i64, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> u64 {
    let days = days_from_civil(year, month, day);
    let second_of_day = u64::from(hour) * 3600 + u64::from(minute) * 60 + u64::from(second);
    (days as u64) * 86_400_000 + second_of_day * 1000
}

fn is_restricted(spec: &str) -> bool {
    spec != "*" && spec != "?"
}

fn normalize_weekday(mask: u64) -> u64 {
    if mask & (1_u64 << 7) != 0 {
        (mask & !(1_u64 << 7)) | 1
    } else {
        mask
    }
}

fn bit_values(mask: u64, max_exclusive: u32) -> impl Iterator<Item = u32> {
    (0..max_exclusive).filter(move |bit| mask & (1_u64 << bit) != 0)
}

fn parse_field(spec: &str, min: u32, max: u32) -> Result<u64> {
    if spec.is_empty() {
        return Err(Error::Protocol("empty cron field"));
    }
    let mut mask = 0_u64;
    for part in spec.split(',') {
        let (range_part, step) = match part.split_once('/') {
            Some((range, step)) => (range, parse_number(step)?),
            None => (part, 1),
        };
        if step == 0 {
            return Err(Error::Protocol("cron step must be > 0"));
        }
        let (start, end) = if range_part == "*" || range_part == "?" {
            (min, max)
        } else if let Some((left, right)) = range_part.split_once('-') {
            (parse_number(left)?, parse_number(right)?)
        } else {
            let value = parse_number(range_part)?;
            (value, value)
        };
        if start > end || start < min || end > max {
            return Err(Error::Protocol("cron field out of range"));
        }
        let mut value = start;
        while value <= end {
            mask |= 1_u64 << value;
            value = match value.checked_add(step) {
                Some(next) => next,
                None => break,
            };
        }
    }
    if mask == 0 {
        return Err(Error::Protocol("cron field matches nothing"));
    }
    Ok(mask)
}

fn parse_number(text: &str) -> Result<u32> {
    text.parse::<u32>()
        .map_err(|_| Error::Protocol("invalid cron number"))
}

// ---- Howard Hinnant 的 civil 日期算法（UTC）----

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_prime = (i64::from(month) + 9) % 12;
    let day_of_year = (153 * month_prime + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_prime + 2) / 5 + 1) as u32;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

fn weekday_from_days(days: i64) -> u32 {
    // 1970-01-01 是星期四；返回 0=周日 .. 6=周六
    (((days % 7) + 7 + 4) % 7) as u32
}
