# SPDX-License-Identifier: MIT
# Copyright (c) 2026 houzc

"""AgentHeart 内嵌承载（FFI / ctypes）冒烟测试（M10）。

直接加载内核 `cdylib`（`agentheart_core.dll` / `libagentheart_core.so` / `libagentheart_core.dylib`），
在**进程内**完成：握手（版本）→ 请求-响应 → 订阅事件 → 拉取事件 → 关闭。

用法：python agentheart-sdk/python/embed_test.py [cdylib 路径]

全部通过时打印 ``EMBED_OK python`` 并以 0 退出。
"""

from __future__ import annotations

import ctypes
import json
import os
import sys
import time

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))


def default_library() -> str:
    if sys.platform == "win32":
        name = "agentheart_core.dll"
    elif sys.platform == "darwin":
        name = "libagentheart_core.dylib"
    else:
        name = "libagentheart_core.so"
    return os.path.join(REPO_ROOT, "target", "release", name)


def load(path: str):
    lib = ctypes.CDLL(path)
    lib.ah_version.restype = ctypes.c_int32
    lib.ah_open.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
    lib.ah_open.restype = ctypes.c_int32
    lib.ah_call.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p)]
    lib.ah_call.restype = ctypes.c_int32
    lib.ah_subscribe.argtypes = [
        ctypes.c_void_p,
        ctypes.c_char_p,
        ctypes.c_uint64,
        ctypes.POINTER(ctypes.c_char_p),
    ]
    lib.ah_subscribe.restype = ctypes.c_int32
    lib.ah_poll.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p)]
    lib.ah_poll.restype = ctypes.c_int32
    lib.ah_string_free.argtypes = [ctypes.c_char_p]
    lib.ah_close.argtypes = [ctypes.c_void_p]
    return lib


class Kernel:
    """内嵌内核句柄（进程内直调 C ABI）。"""

    def __init__(self, lib) -> None:
        self._lib = lib
        self._handle = ctypes.c_void_p()
        code = lib.ah_open(ctypes.byref(self._handle))
        if code != 0:
            raise RuntimeError(f"ah_open 失败：错误码 {code}")

    def _take(self, pointer: ctypes.c_char_p) -> str:
        text = pointer.value.decode("utf-8") if pointer.value else ""
        self._lib.ah_string_free(pointer)
        return text

    def call(self, request) -> dict:
        payload = request if isinstance(request, str) else json.dumps(request, ensure_ascii=False)
        out = ctypes.c_char_p()
        code = self._lib.ah_call(self._handle, payload.encode("utf-8"), ctypes.byref(out))
        if code != 0:
            raise RuntimeError(f"ah_call 失败：错误码 {code}")
        return json.loads(self._take(out))

    def subscribe(self, topics) -> dict:
        out = ctypes.c_char_p()
        payload = json.dumps(list(topics)).encode("utf-8")
        code = self._lib.ah_subscribe(self._handle, payload, 0, ctypes.byref(out))
        if code != 0:
            raise RuntimeError(f"ah_subscribe 失败：错误码 {code}")
        return json.loads(self._take(out))

    def poll(self) -> list:
        out = ctypes.c_char_p()
        code = self._lib.ah_poll(self._handle, ctypes.byref(out))
        if code != 0:
            raise RuntimeError(f"ah_poll 失败：错误码 {code}")
        return json.loads(self._take(out))

    def close(self) -> None:
        if self._handle:
            self._lib.ah_close(self._handle)
            self._handle = ctypes.c_void_p()


def main() -> None:
    path = sys.argv[1] if len(sys.argv) > 1 else default_library()
    if not os.path.exists(path):
        raise AssertionError(f"未找到内核动态库: {path}（请先 cargo build --release）")

    lib = load(path)
    assert lib.ah_version() == 1, "协议版本不符"

    kernel = Kernel(lib)
    print(f"步骤 1 加载 cdylib 并 ah_open：通过（{os.path.basename(path)}）")

    health = kernel.call({"m": "system.health"})
    assert health.get("ok") is True, "system.health 失败"
    print(f"步骤 2 请求-响应：通过（status={health['result']['status']}）")

    assert kernel.subscribe(["task"]).get("ok") is True, "ah_subscribe 失败"
    task_id = kernel.call({"m": "task.trigger", "queue": "embed"})["result"]["taskId"]
    print(f"步骤 3 订阅事件：通过（taskId={task_id}）")

    deadline = time.monotonic() + 5.0
    found = False
    while time.monotonic() < deadline and not found:
        found = any(
            event.get("m") == "event.task" and event.get("state") == "succeeded"
            for event in kernel.poll()
        )
        if not found:
            time.sleep(0.02)
    assert found, "未在期限内收到 event.task/succeeded"
    print("步骤 4 拉取事件：通过（event.task · succeeded）")

    state = kernel.call({"m": "task.get", "taskId": task_id})["result"]["task"]["state"]
    assert state == "succeeded", f"任务状态异常: {state}"
    kernel.close()
    print("步骤 5 关闭句柄：通过")

    print("EMBED_OK python")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # noqa: BLE001 - 冒烟测试统一兜底输出
        print(f"EMBED_FAILED python: {error}", file=sys.stderr)
        sys.exit(1)
