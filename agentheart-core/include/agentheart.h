/* SPDX-License-Identifier: MIT
 * Copyright (c) 2026 houzc
 */

/*
 * AgentHeart 内核 C ABI（零第三方依赖）。
 *
 * 与 `agentheart/src/ffi/mod.rs` 的导出符号一一对应；协议详见方案第 8.3 节。
 * 所有字符串均为 NUL 结尾的 UTF-8。
 */
#ifndef AGENTHEART_H
#define AGENTHEART_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* 不透明内核句柄。 */
typedef struct AhHandle AhHandle;

/* 返回协议版本。 */
int32_t ah_version(void);

/*
 * 创建内核（默认配置）。
 * 成功返回 0 并把句柄写入 *out；失败返回错误码（1 参数非法，10 内部错误）。
 */
int32_t ah_open(AhHandle **out);

/*
 * 处理一次请求。
 *   handle  ：来自 ah_open 的句柄
 *   request ：请求 JSON（NUL 结尾 UTF-8）
 *   out     ：成功时写入响应的 C 字符串（须用 ah_string_free 释放）
 * 返回 0 成功；1 参数非法；6 请求非法；10 内部错误。
 */
int32_t ah_call(AhHandle *handle, const char *request, char **out);

/* 释放 ah_call 返回的字符串。 */
void ah_string_free(char *ptr);

/*
 * 订阅事件流（内嵌承载）。
 *   topics  ：JSON 数组字符串（如 ["task"]；[] 或 ["*"] 表示全部）
 *   from_seq：起始事件序号（0 表示从重放窗口起点）
 *   out     ：成功时写入订阅响应 JSON（须用 ah_string_free 释放）
 * 返回 0 成功；1 参数非法；6 请求非法；10 内部错误。
 */
int32_t ah_subscribe(AhHandle *handle, const char *topics, uint64_t from_seq, char **out);

/*
 * 拉取自上次拉取以来收到的事件（JSON 数组字符串）。
 *   out：成功时写入事件数组 JSON（须用 ah_string_free 释放）
 * 返回 0 成功；1 参数非法；10 内部错误。
 */
int32_t ah_poll(AhHandle *handle, char **out);

/* 关闭并释放内核句柄（幂等）。 */
void ah_close(AhHandle *handle);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* AGENTHEART_H */
