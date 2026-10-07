// AgentHeart Java SDK 冒烟测试：连接侧车并逐项验证 M10 协议能力。
//
// 从环境变量读取 AH_ADDR（形如 127.0.0.1:12345）与 AH_TOKEN；
// 全部步骤成功打印 SMOKE_OK java 并以 0 退出，任一步失败打印错误并以非 0 退出。
//
// 解析 JSON 时不引入第三方库，仅使用字符串查找/截取。
public class Smoke {
    public static void main(String[] args) {
        try {
            run();
            System.out.println("SMOKE_OK java");
        } catch (Throwable error) {
            System.err.println("SMOKE_FAIL java: " + error.getMessage());
            System.exit(1);
        }
    }

    private static void run() throws Exception {
        String addr = System.getenv("AH_ADDR");
        String token = System.getenv("AH_TOKEN");
        if (addr == null || addr.isEmpty()) {
            fail("缺少环境变量 AH_ADDR");
        }
        int colon = addr.lastIndexOf(':');
        if (colon < 0) {
            fail("AH_ADDR 格式非法: " + addr);
        }
        String host = addr.substring(0, colon);
        int port = Integer.parseInt(addr.substring(colon + 1));

        // 1. 连接并握手
        AgentHeartClient client = AgentHeartClient.connect(host, port, token == null ? "" : token);

        // 2. 健康检查
        String health = client.health();
        if (!health.contains("\"ok\":true")) {
            fail("Health 响应缺少 ok=true: " + health);
        }

        // 3. 提交任务并解析 taskId
        String submit = client.submitTask("e2e", "");
        String taskId = stringField(submit, "taskId");
        if (taskId == null || taskId.isEmpty()) {
            fail("SubmitTask 未返回 taskId: " + submit);
        }

        // 4. 轮询任务直至 succeeded（最长 5 秒，每 50ms）
        boolean succeeded = false;
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            String detail = client.getTask(taskId);
            if ("succeeded".equals(stringField(detail, "state"))) {
                succeeded = true;
                break;
            }
            Thread.sleep(50);
        }
        if (!succeeded) {
            fail("任务 " + taskId + " 未在 5 秒内进入 succeeded");
        }

        // 5. 分页：再提交 3 个任务，按 limit=1 用游标翻页，累计 items >= 3
        for (int index = 0; index < 3; index++) {
            client.submitTask("e2e", "");
        }
        int total = 0;
        String cursor = null;
        for (int page = 0; ; page++) {
            if (page > 100) {
                fail("分页翻页次数过多（游标可能未收敛）");
            }
            AgentHeartClient.TaskPage result = client.listTasks(1, cursor);
            total += result.items.size();
            if (result.nextCursor == null || result.nextCursor.isEmpty() || result.items.isEmpty()) {
                break;
            }
            cursor = result.nextCursor;
        }
        if (total < 3) {
            fail("分页累计 items=" + total + "，期望 >= 3");
        }

        // 6. 事件订阅：订阅 task 主题，提交任务后直到收到 event.task + succeeded
        client.subscribe(new String[]{"task"}, 0);
        client.submitTask("e2e", "");
        boolean gotEvent = false;
        deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline && !gotEvent) {
            for (String event : client.drainEvents()) {
                if (event.contains("\"m\":\"event.task\"")
                        && event.contains("\"state\":\"succeeded\"")) {
                    gotEvent = true;
                    break;
                }
            }
            if (!gotEvent) {
                Thread.sleep(50);
            }
        }
        if (!gotEvent) {
            fail("未在 5 秒内收到 event.task(succeeded) 事件");
        }

        // 7. 队列：声明、发布并确认 depth >= 1
        client.declareQueue("e2e-q", 5);
        client.publish("e2e-q", "hi");
        String stats = client.queueStats("e2e-q");
        if (!depthAtLeast(stats, 1)) {
            fail("队列 e2e-q depth 不足（期望 >= 1）: " + stats);
        }

        // 8. 断线重连
        client.close();
        client.reconnect();
        String healthAgain = client.health();
        if (!healthAgain.contains("\"ok\":true")) {
            fail("重连后 Health 响应缺少 ok=true: " + healthAgain);
        }

        // 9. 全部通过（由 main 打印 SMOKE_OK java）
        client.close();
    }

    /** 查找形如 "key":"value" 的字符串字段，返回其值；不存在返回 null。 */
    private static String stringField(String json, String key) {
        String needle = "\"" + key + "\":\"";
        int index = json.indexOf(needle);
        if (index < 0) {
            return null;
        }
        int start = index + needle.length();
        int end = json.indexOf('"', start);
        if (end < 0) {
            return null;
        }
        return json.substring(start, end);
    }

    /** 判断响应中 "depth": 的数值是否 >= min。 */
    private static boolean depthAtLeast(String json, long min) {
        String needle = "\"depth\":";
        int index = json.indexOf(needle);
        if (index < 0) {
            return false;
        }
        int start = index + needle.length();
        int end = start;
        while (end < json.length()) {
            char current = json.charAt(end);
            if (Character.isDigit(current) || current == '-' || current == '+') {
                end++;
            } else {
                break;
            }
        }
        if (end == start) {
            return false;
        }
        try {
            return Long.parseLong(json.substring(start, end)) >= min;
        } catch (NumberFormatException error) {
            return false;
        }
    }

    /** 打印失败信息并退出（退出码 1）。 */
    private static void fail(String message) {
        throw new IllegalStateException(message);
    }
}
