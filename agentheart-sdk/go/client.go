// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

// Package agentheart 提供 AgentHeart 内核接口的 Go 客户端（仅标准库）。
//
// 协议：16 字节小端帧头 + UTF-8 JSON 载荷（见方案第 8.3 节）。
//
// 事件流：建立连接后启动**后台读 goroutine**，事件帧（flags & 4）入事件队列，
// 由 DrainEvents 取走；响应帧按 requestId 分发给等待者。因此同一条连接即可
// 同时完成「请求-响应」与「事件订阅」。
package agentheart

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const (
	magic           = 0x41480001
	protocolVersion = 1
	headerLen       = 16
	flagRequest     = 0x1
	flagResponse    = 0x2
	flagEvent       = 0x4
	flagError       = 0x8

	// dialTimeout 建连超时。
	dialTimeout = 5 * time.Second
	// callTimeout 单次请求等待响应的超时。
	callTimeout = 5 * time.Second
	// maxEvents 事件队列上限（超出丢弃最旧事件）。
	maxEvents = 1024
)

// errClosed 表示连接已关闭或尚未建立。
var errClosed = errors.New("agentheart: connection is closed")

// Client 是内核接口客户端；并发安全。
type Client struct {
	address string
	token   string

	mu      sync.Mutex
	conn    net.Conn
	pending map[uint32]chan []byte
	events  []string
	nextID  uint32
	closed  bool

	writeMu sync.Mutex
}

// Dial 建立连接、启动后台读 goroutine 并完成握手；握手失败返回错误。
func Dial(address, token string) (*Client, error) {
	client := &Client{
		address: address,
		token:   token,
		pending: make(map[uint32]chan []byte),
		nextID:  1,
	}
	if err := client.connect(); err != nil {
		return nil, err
	}
	if err := client.handshake(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

// Call 发送一次请求（原始 JSON 字符串），按 requestId 等待并返回响应 JSON 字符串。
func (c *Client) Call(request string) (string, error) {
	payload := []byte(request)

	c.mu.Lock()
	if c.closed || c.conn == nil {
		c.mu.Unlock()
		return "", errClosed
	}
	requestID := c.nextID
	c.nextID++
	waiter := make(chan []byte, 1)
	c.pending[requestID] = waiter
	conn := c.conn
	c.mu.Unlock()

	if err := c.writeFrame(conn, requestID, payload); err != nil {
		c.mu.Lock()
		delete(c.pending, requestID)
		c.mu.Unlock()
		return "", err
	}

	select {
	case response, ok := <-waiter:
		if !ok {
			return "", errClosed
		}
		return string(response), nil
	case <-time.After(callTimeout):
		c.mu.Lock()
		delete(c.pending, requestID)
		c.mu.Unlock()
		return "", fmt.Errorf("agentheart: response timeout")
	}
}

// Subscribe 订阅事件流（topics 为空或含 "*" 表示全部）。
func (c *Client) Subscribe(topics []string, fromSeq uint64) (string, error) {
	if topics == nil {
		topics = []string{}
	}
	return c.callJSON(map[string]any{
		"m":       "stream.subscribe",
		"topics":  topics,
		"fromSeq": fromSeq,
	})
}

// DrainEvents 返回自上次调用以来收到的事件 JSON 字符串（非阻塞、取空队列）。
func (c *Client) DrainEvents() []string {
	c.mu.Lock()
	events := c.events
	c.events = nil
	c.mu.Unlock()
	if events == nil {
		return []string{}
	}
	return events
}

// Reconnect 关闭旧连接并重新建立连接、重新握手（用于断线重连）。
func (c *Client) Reconnect() error {
	_ = c.Close()
	if err := c.connect(); err != nil {
		return err
	}
	if err := c.handshake(); err != nil {
		_ = c.Close()
		return err
	}
	return nil
}

// Close 关闭连接（幂等）。
func (c *Client) Close() error {
	c.mu.Lock()
	conn := c.conn
	if c.closed || conn == nil {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.conn = nil
	for id, waiter := range c.pending {
		delete(c.pending, id)
		close(waiter)
	}
	c.mu.Unlock()
	return conn.Close()
}

// ---- 类型化便捷方法（返回原始 JSON 字符串）----

// Health 查询系统健康状态（system.health）。
func (c *Client) Health() (string, error) {
	return c.callJSON(map[string]any{"m": "system.health"})
}

// Heartbeat 查询心跳信息（heartbeat.get）。
func (c *Client) Heartbeat() (string, error) {
	return c.callJSON(map[string]any{"m": "heartbeat.get"})
}

// Metrics 查询指标（metrics.get）。
func (c *Client) Metrics() (string, error) {
	return c.callJSON(map[string]any{"m": "metrics.get"})
}

// SubmitTask 即时创建任务（task.trigger），name 为空时不带名称。
func (c *Client) SubmitTask(queue, name string) (string, error) {
	request := map[string]any{"m": "task.trigger", "queue": queue}
	if name != "" {
		request["name"] = name
	}
	return c.callJSON(request)
}

// GetTask 查询单个任务（task.get）。
func (c *Client) GetTask(id string) (string, error) {
	return c.callJSON(map[string]any{"m": "task.get", "taskId": id})
}

// TaskPage 是 task.list 的一页结果。
type TaskPage struct {
	// Items 为任务对象的原始 JSON 字符串。
	Items []string
	// NextCursor 为下一页游标；空字符串表示没有更多。
	NextCursor string
}

// ListTasks 按游标分页列出任务（task.list）。
func (c *Client) ListTasks(limit int, cursor string) (*TaskPage, error) {
	page := map[string]any{"limit": limit}
	if cursor != "" {
		page["cursor"] = cursor
	}
	response, err := c.callJSON(map[string]any{"m": "task.list", "page": page})
	if err != nil {
		return nil, err
	}
	var parsed struct {
		Result struct {
			Items      []json.RawMessage `json:"items"`
			NextCursor *string           `json:"nextCursor"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(response), &parsed); err != nil {
		return nil, err
	}
	result := &TaskPage{Items: make([]string, 0, len(parsed.Result.Items))}
	for _, item := range parsed.Result.Items {
		result.Items = append(result.Items, string(item))
	}
	if parsed.Result.NextCursor != nil {
		result.NextCursor = *parsed.Result.NextCursor
	}
	return result, nil
}

// Jobs 列出定时任务（job.list）。
func (c *Client) Jobs() (string, error) {
	return c.callJSON(map[string]any{"m": "job.list"})
}

// CreateJob 创建定时任务（job.create）。queue 必填，cron 与 intervalMs 二选一；
// 相同 idempotencyKey 返回既有 jobId。
//
// options 使用协议字段名（如 intervalMs、name、enabled、idempotencyKey），留空不下发。
func (c *Client) CreateJob(queue string, options map[string]any) (string, error) {
	request := map[string]any{"m": "job.create", "queue": queue}
	for key, value := range options {
		request[key] = value
	}
	return c.callJSON(request)
}

// DeleteJob 删除定时任务（job.delete）；不存在时内核返回 not_found。
func (c *Client) DeleteJob(jobID string) (string, error) {
	return c.callJSON(map[string]any{"m": "job.delete", "jobId": jobID})
}

// Queues 列出队列（queue.list）。
func (c *Client) Queues() (string, error) {
	return c.callJSON(map[string]any{"m": "queue.list"})
}

// Loops 列出循环任务（loop.list）。
func (c *Client) Loops() (string, error) {
	return c.callJSON(map[string]any{"m": "loop.list"})
}

// Trace 查询任务执行轨迹（trace.get）。
func (c *Client) Trace(taskID string) (string, error) {
	return c.callJSON(map[string]any{"m": "trace.get", "taskId": taskID})
}

// PauseTask 暂停任务（task.pause）。
func (c *Client) PauseTask(id string) (string, error) {
	return c.callJSON(map[string]any{"m": "task.pause", "taskId": id})
}

// ResumeTask 恢复任务（task.resume）。
func (c *Client) ResumeTask(id string) (string, error) {
	return c.callJSON(map[string]any{"m": "task.resume", "taskId": id})
}

// RetryTask 重试任务（task.retry）。
func (c *Client) RetryTask(id string) (string, error) {
	return c.callJSON(map[string]any{"m": "task.retry", "taskId": id})
}

// CancelTask 取消任务（task.cancel）。
func (c *Client) CancelTask(id string) (string, error) {
	return c.callJSON(map[string]any{"m": "task.cancel", "taskId": id})
}

// DeclareQueue 声明队列（queue.declare，幂等）。
func (c *Client) DeclareQueue(name string, capacity int) (string, error) {
	return c.callJSON(map[string]any{
		"m":        "queue.declare",
		"queue":    name,
		"capacity": capacity,
	})
}

// Publish 向队列发布消息（queue.publish）。
func (c *Client) Publish(queue, body string) (string, error) {
	return c.callJSON(map[string]any{
		"m":     "queue.publish",
		"queue": queue,
		"body":  body,
	})
}

// QueueStats 查询队列统计（queue.stats，含 depth）。
func (c *Client) QueueStats(name string) (string, error) {
	request := map[string]any{"m": "queue.stats"}
	if name != "" {
		request["queue"] = name
	}
	return c.callJSON(request)
}

// CreateLoop 创建循环任务（loop.create）。
func (c *Client) CreateLoop(name string, maxIterations, intervalMs int) (string, error) {
	request := map[string]any{
		"m":             "loop.create",
		"maxIterations": maxIterations,
		"intervalMs":    intervalMs,
	}
	if name != "" {
		request["name"] = name
	}
	return c.callJSON(request)
}

// LoopControl 控制循环任务（loop.pause / loop.resume / loop.stop / loop.trigger）。
func (c *Client) LoopControl(action, id string) (string, error) {
	return c.callJSON(map[string]any{
		"m":      "loop." + action,
		"loopId": id,
	})
}

// ---- 内部实现 ----

// connect 建立 TCP 连接并启动后台读 goroutine。
func (c *Client) connect() error {
	conn, err := net.DialTimeout("tcp", c.address, dialTimeout)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.conn = conn
	c.closed = false
	c.pending = make(map[uint32]chan []byte)
	c.events = nil
	c.nextID = 1
	c.mu.Unlock()
	go c.readLoop(conn)
	return nil
}

// handshake 发送 system.hello 并校验响应 ok=true。
func (c *Client) handshake() error {
	request, err := json.Marshal(map[string]any{
		"m":     "system.hello",
		"ver":   protocolVersion,
		"token": c.token,
	})
	if err != nil {
		return err
	}
	response, err := c.Call(string(request))
	if err != nil {
		return err
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(response), &parsed); err != nil {
		return err
	}
	if ok, _ := parsed["ok"].(bool); !ok {
		return fmt.Errorf("agentheart: handshake rejected")
	}
	return nil
}

// readLoop 后台读线程：事件帧入队，响应帧按 requestId 分发；读失败即唤醒所有等待者。
func (c *Client) readLoop(conn net.Conn) {
	for {
		flags, requestID, payload, err := readFrame(conn)
		if err != nil {
			break
		}
		if flags&flagEvent != 0 {
			c.mu.Lock()
			if c.conn == conn {
				c.events = append(c.events, string(payload))
				if len(c.events) > maxEvents {
					c.events = c.events[len(c.events)-maxEvents:]
				}
			}
			c.mu.Unlock()
			continue
		}
		c.mu.Lock()
		waiter, ok := c.pending[requestID]
		if ok {
			delete(c.pending, requestID)
		}
		c.mu.Unlock()
		if ok {
			waiter <- payload
		}
	}
	c.mu.Lock()
	if c.conn == conn {
		c.closed = true
		for id, waiter := range c.pending {
			delete(c.pending, id)
			close(waiter)
		}
	}
	c.mu.Unlock()
}

// callJSON 把请求对象序列化后发送。
func (c *Client) callJSON(request any) (string, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	return c.Call(string(body))
}

// writeFrame 写出一帧请求（串行化写入，避免并发交错）。
func (c *Client) writeFrame(conn net.Conn, requestID uint32, payload []byte) error {
	header := make([]byte, headerLen)
	binary.LittleEndian.PutUint32(header[0:4], magic)
	header[4] = protocolVersion
	header[5] = 0
	binary.LittleEndian.PutUint16(header[6:8], flagRequest)
	binary.LittleEndian.PutUint32(header[8:12], requestID)
	binary.LittleEndian.PutUint32(header[12:16], uint32(len(payload)))

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if _, err := conn.Write(append(header, payload...)); err != nil {
		return err
	}
	return nil
}

// readFrame 从连接读取一帧，返回标志位、requestId 与载荷。
func readFrame(conn net.Conn) (uint16, uint32, []byte, error) {
	header := make([]byte, headerLen)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, 0, nil, err
	}
	if binary.LittleEndian.Uint32(header[0:4]) != magic {
		return 0, 0, nil, fmt.Errorf("agentheart: bad frame magic")
	}
	flags := binary.LittleEndian.Uint16(header[6:8])
	requestID := binary.LittleEndian.Uint32(header[8:12])
	length := binary.LittleEndian.Uint32(header[12:16])
	payload := make([]byte, length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return 0, 0, nil, err
	}
	return flags, requestID, payload, nil
}
