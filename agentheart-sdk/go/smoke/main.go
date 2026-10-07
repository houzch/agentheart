// AgentHeart Go SDK 冒烟测试：连接侧车并逐项验证 M10 协议能力。
//
// 从环境变量读取 AH_ADDR（形如 127.0.0.1:12345）与 AH_TOKEN；
// 全部步骤成功打印 SMOKE_OK go 并以 0 退出，任一步失败打印错误并以 1 退出。
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"agentheart"
)

func main() {
	addr := os.Getenv("AH_ADDR")
	token := os.Getenv("AH_TOKEN")
	if addr == "" {
		fail("缺少环境变量 AH_ADDR")
	}

	// 1. 连接并握手
	client, err := agentheart.Dial(addr, token)
	check("Dial", err)

	// 2. 健康检查
	health, err := client.Health()
	check("Health", err)
	if !isOK(health) {
		fail("Health 响应缺少 ok=true: %s", health)
	}

	// 3. 提交任务并解析 taskId
	submit, err := client.SubmitTask("e2e", "")
	check("SubmitTask", err)
	taskID, _ := lookup(submit, "result", "taskId").(string)
	if taskID == "" {
		fail("SubmitTask 未返回 taskId: %s", submit)
	}

	// 4. 轮询任务直至 succeeded（最长 5 秒）
	deadline := time.Now().Add(5 * time.Second)
	succeeded := false
	for time.Now().Before(deadline) {
		detail, err := client.GetTask(taskID)
		check("GetTask", err)
		if lookup(detail, "result", "task", "state") == "succeeded" {
			succeeded = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !succeeded {
		fail("任务 %s 未在 5 秒内进入 succeeded", taskID)
	}

	// 5. 分页：再提交 3 个任务，按 limit=1 用游标翻页，累计 items >= 3
	for i := 0; i < 3; i++ {
		_, err := client.SubmitTask("e2e", "")
		check("SubmitTask(分页)", err)
	}
	total := 0
	cursor := ""
	for page := 0; ; page++ {
		if page > 100 {
			fail("分页翻页次数过多（游标可能未收敛）")
		}
		result, err := client.ListTasks(1, cursor)
		check("ListTasks", err)
		total += len(result.Items)
		if result.NextCursor == "" || len(result.Items) == 0 {
			break
		}
		cursor = result.NextCursor
	}
	if total < 3 {
		fail("分页累计 items=%d，期望 >= 3", total)
	}

	// 6. 事件订阅：订阅 task 主题，提交任务后直到收到 event.task + succeeded
	_, err = client.Subscribe([]string{"task"}, 0)
	check("Subscribe", err)
	_, err = client.SubmitTask("e2e", "")
	check("SubmitTask(事件)", err)

	gotEvent := false
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !gotEvent {
		for _, raw := range client.DrainEvents() {
			var event map[string]any
			if json.Unmarshal([]byte(raw), &event) != nil {
				continue
			}
			if event["m"] == "event.task" && event["state"] == "succeeded" {
				gotEvent = true
				break
			}
		}
		if !gotEvent {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !gotEvent {
		fail("未在 5 秒内收到 event.task(succeeded) 事件")
	}

	// 7. 队列：声明、发布并确认 depth >= 1
	_, err = client.DeclareQueue("e2e-q", 5)
	check("DeclareQueue", err)
	_, err = client.Publish("e2e-q", "hi")
	check("Publish", err)
	stats, err := client.QueueStats("e2e-q")
	check("QueueStats", err)
	depth := 0.0
	if queues, ok := lookup(stats, "result", "queues").([]any); ok && len(queues) > 0 {
		if entry, ok := queues[0].(map[string]any); ok {
			if value, ok := entry["depth"].(float64); ok {
				depth = value
			}
		}
	}
	if depth < 1 {
		fail("队列 e2e-q depth=%v，期望 >= 1", depth)
	}

	// 8. 断线重连
	check("Close", client.Close())
	check("Reconnect", client.Reconnect())
	health, err = client.Health()
	check("Health(重连后)", err)
	if !isOK(health) {
		fail("重连后 Health 响应缺少 ok=true: %s", health)
	}

	// 9. 全部通过
	fmt.Println("SMOKE_OK go")
}

// check 在出错时终止冒烟测试。
func check(step string, err error) {
	if err != nil {
		fail("%s 失败: %v", step, err)
	}
}

// fail 打印失败信息并退出。
func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "SMOKE_FAIL go: %s\n", fmt.Sprintf(format, args...))
	os.Exit(1)
}

// isOK 判断响应 JSON 的 ok 字段是否为 true。
func isOK(response string) bool {
	var parsed struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal([]byte(response), &parsed); err != nil {
		return false
	}
	return parsed.OK
}

// lookup 逐层取响应 JSON 字段，返回值或 nil。
func lookup(response string, path ...string) any {
	var value any
	if err := json.Unmarshal([]byte(response), &value); err != nil {
		return nil
	}
	for _, key := range path {
		object, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		value = object[key]
	}
	return value
}
