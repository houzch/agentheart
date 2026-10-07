// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

// AgentHeart Java SDK：本地 Socket 客户端（仅 JDK 标准库）。
//
// 协议：16 字节小端帧头 + UTF-8 JSON 载荷（见方案第 8.3 节）。
//
// 事件流：建立连接后启动**后台读线程**（daemon），事件帧（flags & 4）放入事件队列，
// 由 drainEvents 取走；响应帧按 requestId 分发给等待者。因此同一条连接即可同时完成
// 「请求-响应」与「事件订阅」。
//
// 为避免第三方依赖，请求/响应均以 JSON 字符串原样传递。
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public final class AgentHeartClient implements Closeable {
    private static final int MAGIC = 0x41480001;
    private static final int PROTOCOL_VERSION = 1;
    private static final int HEADER_LEN = 16;
    private static final int FLAG_REQUEST = 0x1;
    private static final int FLAG_EVENT = 0x4;

    /** 建连超时（毫秒）。 */
    private static final int CONNECT_TIMEOUT_MS = 5000;
    /** 单次请求等待响应的超时（毫秒）。 */
    private static final long CALL_TIMEOUT_MS = 5000;
    /** 事件队列上限（超出丢弃最旧事件）。 */
    private static final int MAX_EVENTS = 1024;
    /** 读线程结束时投递给等待者的哨兵值，表示连接已断开。 */
    private static final String CLOSED_SENTINEL = "\u0000agentheart-closed\u0000";

    private final String host;
    private final int port;
    private final String token;

    /** 保护 socket/closed/nextId 的锁（连接生命周期）。 */
    private final Object stateLock = new Object();
    /** 串行化写操作，避免并发写帧交错。 */
    private final Object writeLock = new Object();

    /** 每连接独立的待决请求表：requestId -> 响应队列（重连时替换为新实例）。 */
    private ConcurrentHashMap<Integer, ArrayBlockingQueue<String>> pending = new ConcurrentHashMap<>();
    /** 每连接独立的事件队列（供 drainEvents 取走）。 */
    private ConcurrentLinkedQueue<String> events = new ConcurrentLinkedQueue<>();

    private volatile Socket socket;
    private Thread reader;
    private int nextId = 1;
    private boolean closed = true;

    private AgentHeartClient(String host, int port, String token) {
        this.host = host;
        this.port = port;
        this.token = token == null ? "" : token;
    }

    /** 连接、启动后台读线程并完成握手；握手失败抛 IOException。 */
    public static AgentHeartClient connect(String host, int port, String token) throws IOException {
        AgentHeartClient client = new AgentHeartClient(host, port, token);
        try {
            client.open();
            client.handshake();
        } catch (IOException error) {
            client.close();
            throw error;
        }
        return client;
    }

    /** 发送一次请求（原始 JSON 字符串），按 requestId 等待并返回响应 JSON 字符串。 */
    public String call(String requestJson) throws IOException {
        int requestId;
        Socket target;
        ConcurrentHashMap<Integer, ArrayBlockingQueue<String>> table;
        ArrayBlockingQueue<String> waiter = new ArrayBlockingQueue<>(1);
        synchronized (stateLock) {
            if (closed || socket == null) {
                throw new IOException("agentheart: connection is closed");
            }
            target = socket;
            table = pending;
            requestId = nextId++;
            table.put(requestId, waiter);
        }

        try {
            writeFrame(target, requestId, requestJson.getBytes(StandardCharsets.UTF_8));
        } catch (IOException error) {
            table.remove(requestId);
            throw error;
        }

        String response;
        try {
            response = waiter.poll(CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            table.remove(requestId);
            throw new IOException("agentheart: interrupted", error);
        }
        if (response == null) {
            table.remove(requestId);
            throw new IOException("agentheart: response timeout");
        }
        if (CLOSED_SENTINEL.equals(response)) {
            throw new IOException("agentheart: connection is closed");
        }
        return response;
    }

    /** 订阅事件流（topics 为空或含 "*" 表示全部）。 */
    public String subscribe(String[] topics, long fromSeq) throws IOException {
        StringBuilder builder = new StringBuilder("{\"m\":\"stream.subscribe\",\"topics\":[");
        if (topics != null) {
            for (int index = 0; index < topics.length; index++) {
                if (index > 0) {
                    builder.append(',');
                }
                builder.append('"').append(escape(topics[index])).append('"');
            }
        }
        builder.append("],\"fromSeq\":").append(fromSeq).append('}');
        return call(builder.toString());
    }

    /** 返回自上次调用以来收到的事件 JSON 字符串（非阻塞、取空队列）。 */
    public List<String> drainEvents() {
        List<String> drained = new ArrayList<>();
        String event;
        while ((event = events.poll()) != null) {
            drained.add(event);
        }
        return drained;
    }

    /** 关闭旧连接并重新建连、重新握手（用于断线重连）。 */
    public void reconnect() throws IOException {
        close();
        try {
            open();
            handshake();
        } catch (IOException error) {
            close();
            throw error;
        }
    }

    /** 关闭连接（幂等）。 */
    @Override
    public void close() throws IOException {
        Socket target;
        synchronized (stateLock) {
            if (closed) {
                return;
            }
            closed = true;
            target = socket;
            socket = null;
        }
        failPending(pending);
        if (target != null) {
            target.close();
        }
    }

    // ---- 类型化便捷方法（返回原始 JSON 字符串）----

    /** 查询系统健康状态（system.health）。 */
    public String health() throws IOException {
        return call("{\"m\":\"system.health\"}");
    }

    /** 查询心跳信息（heartbeat.get）。 */
    public String heartbeat() throws IOException {
        return call("{\"m\":\"heartbeat.get\"}");
    }

    /** 查询指标（metrics.get）。 */
    public String metrics() throws IOException {
        return call("{\"m\":\"metrics.get\"}");
    }

    /** 即时创建任务（task.trigger），name 为空时不带名称。 */
    public String submitTask(String queue, String name) throws IOException {
        if (name == null || name.isEmpty()) {
            return call("{\"m\":\"task.trigger\",\"queue\":\"" + escape(queue) + "\"}");
        }
        return call("{\"m\":\"task.trigger\",\"queue\":\"" + escape(queue)
                + "\",\"name\":\"" + escape(name) + "\"}");
    }

    /** 查询单个任务（task.get）。 */
    public String getTask(String id) throws IOException {
        return call("{\"m\":\"task.get\",\"taskId\":\"" + escape(id) + "\"}");
    }

    /** task.list 的一页结果。 */
    public static final class TaskPage {
        /** 任务对象的原始 JSON 字符串列表。 */
        public final List<String> items;
        /** 下一页游标；null 或空表示没有更多。 */
        public final String nextCursor;

        public TaskPage(List<String> items, String nextCursor) {
            this.items = items;
            this.nextCursor = nextCursor;
        }
    }

    /** 按游标分页列出任务（task.list）。 */
    public TaskPage listTasks(int limit, String cursor) throws IOException {
        StringBuilder builder = new StringBuilder("{\"m\":\"task.list\",\"page\":{\"limit\":").append(limit);
        if (cursor != null && !cursor.isEmpty()) {
            builder.append(",\"cursor\":\"").append(escape(cursor)).append('"');
        }
        builder.append("}}");
        return parseTaskPage(call(builder.toString()));
    }

    /** 列出定时任务（job.list）。 */
    public String jobs() throws IOException {
        return call("{\"m\":\"job.list\"}");
    }

    /**
     * 创建定时任务（job.create）。
     *
     * <p>{@code queue} 必填，{@code cron} 与 {@code intervalMs} 二选一；
     * 相同 {@code idempotencyKey} 返回既有 jobId。为 null 的字段不下发。
     */
    public String createJob(
            String queue,
            String cron,
            Long intervalMs,
            String name,
            Boolean enabled,
            String idempotencyKey) throws IOException {
        StringBuilder builder = new StringBuilder("{\"m\":\"job.create\",\"queue\":\"")
                .append(escape(queue))
                .append('"');
        if (name != null && !name.isEmpty()) {
            builder.append(",\"name\":\"").append(escape(name)).append('"');
        }
        if (cron != null && !cron.isEmpty()) {
            builder.append(",\"cron\":\"").append(escape(cron)).append('"');
        }
        if (intervalMs != null) {
            builder.append(",\"intervalMs\":").append(intervalMs);
        }
        if (enabled != null) {
            builder.append(",\"enabled\":").append(enabled);
        }
        if (idempotencyKey != null && !idempotencyKey.isEmpty()) {
            builder.append(",\"idempotencyKey\":\"").append(escape(idempotencyKey)).append('"');
        }
        builder.append('}');
        return call(builder.toString());
    }

    /** 删除定时任务（job.delete）；不存在时内核返回 not_found。 */
    public String deleteJob(String jobId) throws IOException {
        return call("{\"m\":\"job.delete\",\"jobId\":\"" + escape(jobId) + "\"}");
    }

    /** 列出队列（queue.list）。 */
    public String queues() throws IOException {
        return call("{\"m\":\"queue.list\"}");
    }

    /** 查询队列统计（queue.stats，含 depth）。 */
    public String queueStats(String queue) throws IOException {
        if (queue == null || queue.isEmpty()) {
            return call("{\"m\":\"queue.stats\"}");
        }
        return call("{\"m\":\"queue.stats\",\"queue\":\"" + escape(queue) + "\"}");
    }

    /** 列出循环任务（loop.list）。 */
    public String loops() throws IOException {
        return call("{\"m\":\"loop.list\"}");
    }

    /** 查询任务执行轨迹（trace.get）。 */
    public String trace(String taskId) throws IOException {
        return call("{\"m\":\"trace.get\",\"taskId\":\"" + escape(taskId) + "\"}");
    }

    /** 暂停任务（task.pause）。 */
    public String pauseTask(String id) throws IOException {
        return call("{\"m\":\"task.pause\",\"taskId\":\"" + escape(id) + "\"}");
    }

    /** 恢复任务（task.resume）。 */
    public String resumeTask(String id) throws IOException {
        return call("{\"m\":\"task.resume\",\"taskId\":\"" + escape(id) + "\"}");
    }

    /** 重试任务（task.retry）。 */
    public String retryTask(String id) throws IOException {
        return call("{\"m\":\"task.retry\",\"taskId\":\"" + escape(id) + "\"}");
    }

    /** 取消任务（task.cancel）。 */
    public String cancelTask(String id) throws IOException {
        return call("{\"m\":\"task.cancel\",\"taskId\":\"" + escape(id) + "\"}");
    }

    /** 声明队列（queue.declare，幂等）。 */
    public String declareQueue(String name, int capacity) throws IOException {
        return call("{\"m\":\"queue.declare\",\"queue\":\"" + escape(name)
                + "\",\"capacity\":" + capacity + "}");
    }

    /** 向队列发布消息（queue.publish）。 */
    public String publish(String queue, String body) throws IOException {
        return call("{\"m\":\"queue.publish\",\"queue\":\"" + escape(queue)
                + "\",\"body\":\"" + escape(body) + "\"}");
    }

    /** 创建循环任务（loop.create）。 */
    public String createLoop(String name, int maxIterations, int intervalMs) throws IOException {
        StringBuilder builder = new StringBuilder("{\"m\":\"loop.create\"");
        if (name != null && !name.isEmpty()) {
            builder.append(",\"name\":\"").append(escape(name)).append('"');
        }
        builder.append(",\"maxIterations\":").append(maxIterations)
                .append(",\"intervalMs\":").append(intervalMs).append('}');
        return call(builder.toString());
    }

    /** 控制循环任务（loop.pause / loop.resume / loop.stop / loop.trigger）。 */
    public String loopControl(String action, String id) throws IOException {
        return call("{\"m\":\"loop." + action + "\",\"loopId\":\"" + escape(id) + "\"}");
    }

    // ---- 内部实现 ----

    /** 建立 TCP 连接并启动后台读线程。 */
    private void open() throws IOException {
        Socket target = new Socket();
        try {
            target.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
        } catch (IOException error) {
            try {
                target.close();
            } catch (IOException ignored) {
                // 忽略关闭异常
            }
            throw error;
        }
        ConcurrentHashMap<Integer, ArrayBlockingQueue<String>> table = new ConcurrentHashMap<>();
        ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
        synchronized (stateLock) {
            socket = target;
            closed = false;
            nextId = 1;
            pending = table;
            events = queue;
        }
        Thread thread = new Thread(() -> readLoop(target, table, queue), "agentheart-reader");
        thread.setDaemon(true);
        reader = thread;
        thread.start();
    }

    /** 发送 system.hello 并校验响应 ok=true。 */
    private void handshake() throws IOException {
        String response = call("{\"m\":\"system.hello\",\"ver\":" + PROTOCOL_VERSION
                + ",\"token\":\"" + escape(token) + "\"}");
        if (!response.contains("\"ok\":true")) {
            throw new IOException("agentheart: handshake rejected");
        }
    }

    /** 后台读线程：只读写本连接的队列；连接结束后唤醒本连接的等待者。 */
    private void readLoop(Socket target,
            ConcurrentHashMap<Integer, ArrayBlockingQueue<String>> table,
            ConcurrentLinkedQueue<String> queue) {
        try {
            while (true) {
                Frame frame = readFrame(target);
                if ((frame.flags & FLAG_EVENT) != 0) {
                    queue.add(new String(frame.payload, StandardCharsets.UTF_8));
                    while (queue.size() > MAX_EVENTS) {
                        queue.poll();
                    }
                    continue;
                }
                ArrayBlockingQueue<String> waiter = table.remove(frame.requestId);
                if (waiter != null) {
                    waiter.offer(new String(frame.payload, StandardCharsets.UTF_8));
                }
            }
        } catch (IOException ignored) {
            // 连接断开或关闭，进入 finally 唤醒本连接的等待者
        } finally {
            failPending(table);
        }
    }

    /** 唤醒指定连接上所有等待者，令其感知连接已断开。 */
    private static void failPending(ConcurrentHashMap<Integer, ArrayBlockingQueue<String>> table) {
        for (Integer id : table.keySet()) {
            ArrayBlockingQueue<String> waiter = table.remove(id);
            if (waiter != null) {
                waiter.offer(CLOSED_SENTINEL);
            }
        }
    }

    /** 写出一帧请求（串行化写入，避免并发交错）。 */
    private void writeFrame(Socket target, int requestId, byte[] payload) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(HEADER_LEN).order(ByteOrder.LITTLE_ENDIAN);
        header.putInt(MAGIC)
                .put((byte) PROTOCOL_VERSION)
                .put((byte) 0)
                .putShort((short) FLAG_REQUEST)
                .putInt(requestId)
                .putInt(payload.length);
        synchronized (writeLock) {
            OutputStream out = target.getOutputStream();
            out.write(header.array());
            out.write(payload);
            out.flush();
        }
    }

    /** 从连接读取一帧。 */
    private Frame readFrame(Socket target) throws IOException {
        byte[] header = readExact(target, HEADER_LEN);
        ByteBuffer buffer = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
        if (buffer.getInt() != MAGIC) {
            throw new IOException("agentheart: bad frame magic");
        }
        buffer.get();
        buffer.get();
        int flags = buffer.getShort() & 0xFFFF;
        int requestId = buffer.getInt();
        int length = buffer.getInt();
        return new Frame(flags, requestId, readExact(target, length));
    }

    /** 精确读取 count 字节。 */
    private byte[] readExact(Socket target, int count) throws IOException {
        byte[] out = new byte[count];
        InputStream in = target.getInputStream();
        int read = 0;
        while (read < count) {
            int n = in.read(out, read, count - read);
            if (n < 0) {
                throw new IOException("agentheart: connection closed");
            }
            read += n;
        }
        return out;
    }

    /** 解析 task.list 响应中的 items 与 nextCursor（不依赖第三方 JSON 库）。 */
    private static TaskPage parseTaskPage(String response) {
        List<String> items = new ArrayList<>();
        int key = response.indexOf("\"items\"");
        if (key >= 0) {
            int open = response.indexOf('[', key);
            int close = open < 0 ? -1 : findContainerEnd(response, open);
            if (open >= 0 && close > open) {
                int index = open + 1;
                while (index < close) {
                    char current = response.charAt(index);
                    if (current == ',' || Character.isWhitespace(current)) {
                        index++;
                        continue;
                    }
                    int end = findValueEnd(response, index);
                    items.add(response.substring(index, end + 1));
                    index = end + 1;
                }
            }
        }
        return new TaskPage(items, extractStringField(response, "nextCursor"));
    }

    /** 定位以 start 处的 '{' 或 '[' 开始、与之配对的闭合符号下标。 */
    private static int findContainerEnd(String text, int start) {
        int depth = 0;
        boolean inString = false;
        for (int index = start; index < text.length(); index++) {
            char current = text.charAt(index);
            if (inString) {
                if (current == '\\') {
                    index++;
                } else if (current == '"') {
                    inString = false;
                }
                continue;
            }
            if (current == '"') {
                inString = true;
            } else if (current == '{' || current == '[') {
                depth++;
            } else if (current == '}' || current == ']') {
                depth--;
                if (depth == 0) {
                    return index;
                }
            }
        }
        return -1;
    }

    /** 返回从 start 开始的 JSON 值（对象/数组/字符串/标量）的结束下标。 */
    private static int findValueEnd(String text, int start) {
        char first = text.charAt(start);
        if (first == '{' || first == '[') {
            return findContainerEnd(text, start);
        }
        if (first == '"') {
            int index = start + 1;
            while (index < text.length()) {
                char current = text.charAt(index);
                if (current == '\\') {
                    index += 2;
                    continue;
                }
                if (current == '"') {
                    return index;
                }
                index++;
            }
            return text.length() - 1;
        }
        int index = start;
        while (index < text.length()) {
            char current = text.charAt(index);
            if (current == ',' || current == ']' || current == '}'
                    || Character.isWhitespace(current)) {
                break;
            }
            index++;
        }
        return index - 1;
    }

    /** 提取形如 "key":"value" 的字符串字段；值为 null 或不存在时返回 null。 */
    private static String extractStringField(String text, String key) {
        String needle = "\"" + key + "\"";
        int keyIndex = text.indexOf(needle);
        if (keyIndex < 0) {
            return null;
        }
        int colon = text.indexOf(':', keyIndex + needle.length());
        if (colon < 0) {
            return null;
        }
        int index = colon + 1;
        while (index < text.length() && Character.isWhitespace(text.charAt(index))) {
            index++;
        }
        if (index >= text.length() || text.charAt(index) != '"') {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        index++;
        while (index < text.length()) {
            char current = text.charAt(index);
            if (current == '\\') {
                index++;
                if (index < text.length()) {
                    builder.append(text.charAt(index));
                    index++;
                }
                continue;
            }
            if (current == '"') {
                break;
            }
            builder.append(current);
            index++;
        }
        return builder.toString();
    }

    /** 最小化的 JSON 字符串转义。 */
    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder builder = new StringBuilder(value.length() + 8);
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            switch (current) {
                case '"':
                    builder.append("\\\"");
                    break;
                case '\\':
                    builder.append("\\\\");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    builder.append(current);
            }
        }
        return builder.toString();
    }

    private static final class Frame {
        final int flags;
        final int requestId;
        final byte[] payload;

        Frame(int flags, int requestId, byte[] payload) {
            this.flags = flags;
            this.requestId = requestId;
            this.payload = payload;
        }
    }
}
