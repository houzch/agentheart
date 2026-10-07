// SPDX-License-Identifier: MIT
// Copyright (c) 2026 houzc

// AgentHeart Java SDK 内嵌承载（FFI）冒烟测试。
//
// 使用 JDK 22+ 的 Foreign Function & Memory API（java.lang.foreign）直接加载内核
// cdylib（target/release/agentheart_core.dll），在进程内调用 C ABI；
// 无需 JNI，也无需 C 编译器。
//
// 运行要求：
//   java --enable-native-access=ALL-UNNAMED -cp <classes> Embed [动态库路径]
//
//   * FFM 自 JDK 22 起即为正式 API，**不需要** --enable-preview。
//   * --enable-native-access=ALL-UNNAMED 是必需的：SymbolLookup.libraryLookup /
//     MemorySegment.reinterpret 等属于受限方法，未显式开启时运行期会告警，
//     并在未来版本中可能直接抛异常。
//
// 全部步骤成功打印 EMBED_OK java 并以 0 退出；任一步失败打印
// EMBED_FAILED java: ... 到 stderr 并以 1 退出。
//
// 解析 JSON 时不引入第三方库，仅使用字符串查找/截取（与 Smoke.java 保持一致）。
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;

public class Embed {

    public static void main(String[] args) {
        try {
            run(args);
            System.out.println("EMBED_OK java");
        } catch (Throwable error) {
            String message = error.getMessage();
            System.err.println("EMBED_FAILED java: " + (message == null ? error : message));
            System.exit(1);
        }
    }

    private static void run(String[] args) throws Throwable {
        String library = resolveLibrary(args);

        // 单个 Arena 覆盖整个生命周期：符号查找与其上分配的内存都依赖它存活。
        try (Arena arena = Arena.ofConfined()) {
            Linker linker = Linker.nativeLinker();
            SymbolLookup lookup = SymbolLookup.libraryLookup(Path.of(library), arena);

            // 绑定 C ABI 的 7 个导出符号。
            MethodHandle ahVersion = linker.downcallHandle(
                    lookup.find("ah_version").orElseThrow(), FunctionDescriptor.of(JAVA_INT));
            MethodHandle ahOpen = linker.downcallHandle(
                    lookup.find("ah_open").orElseThrow(), FunctionDescriptor.of(JAVA_INT, ADDRESS));
            MethodHandle ahCall = linker.downcallHandle(
                    lookup.find("ah_call").orElseThrow(),
                    FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS));
            MethodHandle ahSubscribe = linker.downcallHandle(
                    lookup.find("ah_subscribe").orElseThrow(),
                    FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS));
            MethodHandle ahPoll = linker.downcallHandle(
                    lookup.find("ah_poll").orElseThrow(), FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
            MethodHandle ahStringFree = linker.downcallHandle(
                    lookup.find("ah_string_free").orElseThrow(), FunctionDescriptor.ofVoid(ADDRESS));
            MethodHandle ahClose = linker.downcallHandle(
                    lookup.find("ah_close").orElseThrow(), FunctionDescriptor.ofVoid(ADDRESS));

            // 1. 版本号必须为 1。
            int version = (int) ahVersion.invokeExact();
            if (version != 1) {
                fail("ah_version=" + version + "，期望 1");
            }

            // 2. 打开内核句柄（ah_open 通过 AhHandle **out 写回指针）。
            MemorySegment outPtr = arena.allocate(ADDRESS);
            check((int) ahOpen.invokeExact(outPtr), "ah_open");
            MemorySegment handle = outPtr.get(ADDRESS, 0);
            if (handle.address() == 0L) {
                fail("ah_open 返回空句柄");
            }

            try {
                // 3. 健康检查。
                String health = call(arena, ahCall, ahStringFree, handle, "{\"m\":\"system.health\"}");
                if (!health.contains("\"ok\":true")) {
                    fail("system.health 缺少 ok=true: " + health);
                }

                // 4. 订阅 task 主题（from_seq = 0）。
                MemorySegment topics = arena.allocateFrom("[\"task\"]");
                check((int) ahSubscribe.invokeExact(handle, topics, 0L, outPtr), "ah_subscribe");
                String subscribed = release(ahStringFree, outPtr.get(ADDRESS, 0));
                if (!subscribed.contains("\"ok\":true")) {
                    fail("ah_subscribe 缺少 ok=true: " + subscribed);
                }

                // 5. 触发任务并解析出 result.taskId。
                String triggered = call(arena, ahCall, ahStringFree, handle,
                        "{\"m\":\"task.trigger\",\"queue\":\"embed\"}");
                String taskId = stringField(triggered, "taskId");
                if (taskId == null || taskId.isEmpty()) {
                    fail("task.trigger 未返回 taskId: " + triggered);
                }

                // 6. 轮询事件，直到出现同时含 event.task 与 succeeded 的片段（每 20ms，最长 5 秒）。
                boolean gotEvent = false;
                long deadline = System.currentTimeMillis() + 5000;
                while (System.currentTimeMillis() < deadline && !gotEvent) {
                    check((int) ahPoll.invokeExact(handle, outPtr), "ah_poll");
                    String events = release(ahStringFree, outPtr.get(ADDRESS, 0));
                    if (events.contains("event.task") && events.contains("succeeded")) {
                        gotEvent = true;
                        break;
                    }
                    Thread.sleep(20);
                }
                if (!gotEvent) {
                    fail("未在 5 秒内收到 event.task(succeeded) 事件");
                }

                // 7. 按 taskId 复查任务状态。
                String detail = call(arena, ahCall, ahStringFree, handle,
                        "{\"m\":\"task.get\",\"taskId\":\"" + taskId + "\"}");
                if (!detail.contains("succeeded")) {
                    fail("task.get 缺少 succeeded: " + detail);
                }
            } finally {
                // 8. 关闭句柄。
                ahClose.invokeExact(handle);
            }
        }
        // 9. 全部通过（由 main 打印 EMBED_OK java）。
    }

    /** 调用 ah_call，返回响应 JSON 字符串；失败即抛出。 */
    private static String call(Arena arena, MethodHandle ahCall, MethodHandle ahStringFree,
            MemorySegment handle, String request) throws Throwable {
        MemorySegment requestPtr = arena.allocateFrom(request);
        MemorySegment outPtr = arena.allocate(ADDRESS);
        int code = (int) ahCall.invokeExact(handle, requestPtr, outPtr);
        if (code != 0) {
            fail("ah_call(" + request + ") 返回错误码 " + code);
        }
        return release(ahStringFree, outPtr.get(ADDRESS, 0));
    }

    /** 先读出 NUL 结尾 UTF-8 字符串内容，再调用 ah_string_free 释放。 */
    private static String release(MethodHandle ahStringFree, MemorySegment ptr) throws Throwable {
        if (ptr == null || ptr.address() == 0L) {
            return "";
        }
        // reinterpret 到无界长度以读取 NUL 结尾字符串。
        String text = ptr.reinterpret(Long.MAX_VALUE).getString(0);
        ahStringFree.invokeExact(ptr);
        return text;
    }

    /** 判断错误码并在非 0 时抛出。 */
    private static void check(int code, String what) {
        if (code != 0) {
            fail(what + " 返回错误码 " + code);
        }
    }

    /**
     * 解析动态库路径：优先 args[0]；否则用 user.dir/target/release/agentheart_core.dll，
     * 找不到时回退 user.dir/../../target/release/agentheart_core.dll。
     */
    private static String resolveLibrary(String[] args) {
        if (args.length > 0 && args[0] != null && !args[0].isEmpty()) {
            Path explicit = Path.of(args[0]).toAbsolutePath().normalize();
            if (!Files.isRegularFile(explicit)) {
                fail("动态库不存在: " + explicit);
            }
            return explicit.toString();
        }
        Path base = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        Path[] candidates = {
            base.resolve("target").resolve("release").resolve("agentheart_core.dll"),
            base.resolve("..").resolve("..").resolve("target").resolve("release").resolve("agentheart_core.dll"),
        };
        for (Path candidate : candidates) {
            Path normalized = candidate.normalize();
            if (Files.isRegularFile(normalized)) {
                return normalized.toString();
            }
        }
        fail("找不到 agentheart_core.dll，尝试过: "
                + candidates[0].normalize() + " 与 " + candidates[1].normalize());
        return null;
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

    /** 抛出失败（由 main 统一打印并退出）。 */
    private static void fail(String message) {
        throw new IllegalStateException(message);
    }
}
