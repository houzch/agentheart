# SPDX-License-Identifier: MIT
# Copyright (c) 2026 houzc

"""AgentHeart Python SDK：内核接口客户端（仅标准库）。

协议：16 字节小端帧头 + UTF-8 JSON 载荷（见方案第 8.3 节）。

特性：
- **单连接事件流**：后台读线程解析数据帧，事件帧入队（``drain_events``），
  响应帧按 requestId 分发给等待者；
- **类型化便捷方法**：任务 / 定时 / 队列 / 循环 / 链路 / 指标；
- **断线重连**：``reconnect()`` 重建连接并重新握手（连接级状态独立，互不污染）。

用法：
    client = Client.connect("127.0.0.1:17890", token="...")
    print(client.health())
    client.subscribe(["task"])
    ...
    client.close()
"""

from __future__ import annotations

import json
import queue
import socket
import struct
import threading

MAGIC = 0x41480001
PROTOCOL_VERSION = 1
HEADER = struct.Struct("<IBBHII")  # magic / ver / type / flags / request_id / length
FLAG_REQUEST = 0x1
FLAG_EVENT = 0x4
MAX_EVENTS = 1024
DEFAULT_TIMEOUT = 5.0

_CLOSED = object()


class _Connection:
    """单连接的收发状态（重连时整体替换，旧状态不再影响新连接）。"""

    def __init__(self, sock: socket.socket, timeout: float) -> None:
        self._sock = sock
        self._timeout = timeout
        self._lock = threading.Lock()
        self._pending: dict[int, queue.Queue] = {}
        self._events: list[dict] = []
        self._next_id = 1
        self._closed = False
        self._reader = threading.Thread(target=self._read_loop, name="agentheart-py-reader", daemon=True)
        self._reader.start()

    # ---- 读线程 ----

    def _read_exact(self, count: int) -> bytes:
        buffer = bytearray()
        while len(buffer) < count:
            chunk = self._sock.recv(count - len(buffer))
            if not chunk:
                raise ConnectionError("连接已关闭")
            buffer.extend(chunk)
        return bytes(buffer)

    def _read_loop(self) -> None:
        try:
            while True:
                magic, _ver, _type, flags, request_id, length = HEADER.unpack(
                    self._read_exact(HEADER.size)
                )
                if magic != MAGIC:
                    raise ValueError("帧魔数非法")
                body = self._read_exact(length) if length else b""
                if not body:
                    continue
                try:
                    value = json.loads(body.decode("utf-8"))
                except json.JSONDecodeError:
                    continue
                if flags & FLAG_EVENT:
                    with self._lock:
                        self._events.append(value)
                        if len(self._events) > MAX_EVENTS:
                            self._events.pop(0)
                    continue
                with self._lock:
                    waiter = self._pending.pop(request_id, None)
                if waiter is not None:
                    waiter.put(value)
        except (OSError, ValueError, ConnectionError):
            pass
        finally:
            self._fail()

    def _fail(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            waiters = list(self._pending.values())
            self._pending.clear()
        for waiter in waiters:
            waiter.put(_CLOSED)

    # ---- 请求 / 事件 ----

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def request(self, payload: str) -> dict:
        body = payload.encode("utf-8")
        with self._lock:
            if self._closed:
                raise ConnectionError("内核连接已关闭")
            request_id = self._next_id
            self._next_id += 1
            waiter: queue.Queue = queue.Queue(1)
            self._pending[request_id] = waiter
        header = HEADER.pack(MAGIC, PROTOCOL_VERSION, 0, FLAG_REQUEST, request_id, len(body))
        try:
            self._sock.sendall(header + body)
        except OSError as error:
            with self._lock:
                self._pending.pop(request_id, None)
            raise ConnectionError(f"写入失败: {error}") from error
        try:
            result = waiter.get(timeout=self._timeout)
        except queue.Empty as error:
            with self._lock:
                self._pending.pop(request_id, None)
            raise TimeoutError("内核响应超时") from error
        if result is _CLOSED:
            raise ConnectionError("内核连接已关闭")
        return result

    def drain(self) -> list[dict]:
        with self._lock:
            events = self._events
            self._events = []
        return events

    def close(self) -> None:
        try:
            self._sock.close()
        finally:
            self._fail()


class Client:
    """内核接口客户端。"""

    def __init__(self, address: str, token: str = "", timeout: float = DEFAULT_TIMEOUT) -> None:
        self._address = address
        self._token = token
        self._timeout = timeout
        self._connection: _Connection | None = None

    @classmethod
    def connect(cls, address: str, token: str = "", timeout: float = DEFAULT_TIMEOUT) -> "Client":
        """连接并完成握手。"""
        client = cls(address, token, timeout)
        client.reconnect()
        return client

    def reconnect(self) -> None:
        """建立底层连接并握手（亦用于断线重连）。"""
        host, _, port = self._address.rpartition(":")
        if not host:
            raise ValueError(f"地址非法: {self._address}")
        sock = socket.create_connection((host, int(port)), self._timeout)
        sock.settimeout(None)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._connection = _Connection(sock, self._timeout)
        response = self.call(
            {"m": "system.hello", "ver": PROTOCOL_VERSION, "token": self._token}
        )
        if response.get("ok") is not True:
            raise ConnectionError("agentheart: 握手被拒绝")

    def call(self, request) -> dict:
        """发送一次请求并返回响应对象（可传 dict 或 JSON 字符串）。"""
        if self._connection is None:
            raise ConnectionError("尚未连接内核")
        payload = request if isinstance(request, str) else json.dumps(request, ensure_ascii=False)
        return self._connection.request(payload)

    def subscribe(self, topics, from_seq: int = 0) -> dict:
        """订阅事件流（``topics`` 为空或含 ``"*"`` 表示全部）。"""
        return self.call({"m": "stream.subscribe", "topics": list(topics), "fromSeq": from_seq})

    def drain_events(self) -> list[dict]:
        """取走自上次调用以来收到的事件（非阻塞）。"""
        return [] if self._connection is None else self._connection.drain()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()

    # ---- 类型化便捷方法 ----

    def health(self) -> dict:
        return self.call({"m": "system.health"})

    def heartbeat(self) -> dict:
        return self.call({"m": "heartbeat.get"})

    def metrics(self) -> dict:
        return self.call({"m": "metrics.get"})

    def trace(self, task_id: str) -> dict:
        return self.call({"m": "trace.get", "taskId": task_id})

    def submit_task(self, queue_name: str, name: str | None = None) -> dict:
        request = {"m": "task.trigger", "queue": queue_name}
        if name:
            request["name"] = name
        return self.call(request)

    def get_task(self, task_id: str) -> dict:
        return self.call({"m": "task.get", "taskId": task_id})

    def pause_task(self, task_id: str) -> dict:
        return self.call({"m": "task.pause", "taskId": task_id})

    def resume_task(self, task_id: str) -> dict:
        return self.call({"m": "task.resume", "taskId": task_id})

    def retry_task(self, task_id: str, reset_attempts: bool = False) -> dict:
        return self.call({"m": "task.retry", "taskId": task_id, "resetAttempts": reset_attempts})

    def cancel_task(self, task_id: str) -> dict:
        return self.call({"m": "task.cancel", "taskId": task_id})

    def list_tasks(self, limit: int = 50, cursor: str | None = None) -> dict:
        """游标分页：返回 ``{"items": [...], "nextCursor": str | None}``。"""
        page: dict = {"limit": limit}
        if cursor:
            page["cursor"] = cursor
        result = self.call({"m": "task.list", "page": page}).get("result") or {}
        return {"items": result.get("items") or [], "nextCursor": result.get("nextCursor")}

    def jobs(self) -> dict:
        return self.call({"m": "job.list"})

    def create_job(
        self,
        queue_name: str,
        cron: str | None = None,
        interval_ms: int | None = None,
        name: str | None = None,
        misfire_policy: str | None = None,
        max_attempts: int | None = None,
        max_consecutive_failures: int | None = None,
        enabled: bool | None = None,
        idempotency_key: str | None = None,
    ) -> dict:
        """创建定时任务（job.create）。

        ``queue_name`` 必填，``cron`` 与 ``interval_ms`` 二选一；
        相同 ``idempotency_key`` 返回既有 jobId。
        """
        request: dict = {"m": "job.create", "queue": queue_name}
        if name:
            request["name"] = name
        if cron:
            request["cron"] = cron
        if interval_ms is not None:
            request["intervalMs"] = interval_ms
        if misfire_policy:
            request["misfirePolicy"] = misfire_policy
        if max_attempts is not None:
            request["maxAttempts"] = max_attempts
        if max_consecutive_failures is not None:
            request["maxConsecutiveFailures"] = max_consecutive_failures
        if enabled is not None:
            request["enabled"] = enabled
        if idempotency_key:
            request["idempotencyKey"] = idempotency_key
        return self.call(request)

    def delete_job(self, job_id: str) -> dict:
        """删除定时任务（job.delete）；不存在时内核返回 not_found。"""
        return self.call({"m": "job.delete", "jobId": job_id})

    def queues(self) -> dict:
        return self.call({"m": "queue.list"})

    def queue_stats(self, queue_name: str) -> dict:
        return self.call({"m": "queue.stats", "queue": queue_name})

    def declare_queue(self, queue_name: str, capacity: int | None = None) -> dict:
        request = {"m": "queue.declare", "queue": queue_name}
        if capacity:
            request["capacity"] = capacity
        return self.call(request)

    def publish(self, queue_name: str, body: str) -> dict:
        return self.call({"m": "queue.publish", "queue": queue_name, "body": body})

    def loops(self) -> dict:
        return self.call({"m": "loop.list"})

    def create_loop(self, name: str | None, max_iterations: int, interval_ms: int = 0) -> dict:
        request = {"m": "loop.create", "maxIterations": max_iterations, "intervalMs": interval_ms}
        if name:
            request["name"] = name
        return self.call(request)

    def loop_control(self, action: str, loop_id: str) -> dict:
        return self.call({"m": f"loop.{action}", "loopId": loop_id})
