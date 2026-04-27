# JDK 21 升级改造总结

## 一、背景

将 RocketMQ Broker 的 Send/Pull/ACK 流程以及 Proxy Remoting 协议层从传统的 `ThreadPoolExecutor` + `CompletableFuture` 回调模式，
升级为 **JDK 21 虚拟线程 (Virtual Threads)** + **同步编程模型**，降低代码复杂度并提升高并发场景下的 P99 延迟表现。

## 二、修改文件清单

| 模块 | 文件 | 改动说明 |
|------|------|----------|
| 父 pom | `pom.xml` | 编译器从 `<source>`/`<target>` 改为 `<release>`，添加 `--enable-preview`；Surefire 添加 `--enable-preview` 参数 |
| 父 pom | `pom.xml` | JaCoCo 0.8.5 不支持 JDK 21 class file (major 65)，需跳过或升级版本 |
| 代理模块 | `proxy/pom.xml` | `maven.compiler.source/target` 从 `8` 改为 `21` |
| 公共模块 | `common/BrokerConfig.java` | 新增 `enableVirtualThread` 配置（默认 `true`） |
| 公共模块 | `common/ThreadUtils.java` | 新增 `newVirtualThreadPerTaskExecutor()` 工厂方法 |
| Broker 模块 | `broker/BrokerController.java` | `sendMessageExecutor`、`pullMessageExecutor` 和 `ackMessageExecutor` 条件切换为虚拟线程 |
| Broker 模块 | `broker/SendMessageProcessor.java` | 新增虚拟线程同步路径，用 `.join()` 替代 `thenAcceptAsync` 回调 |
| Broker 模块 | `broker/AckMessageProcessor.java` | 新增虚拟线程同步路径，用 `.join()` 替代 `thenAccept`/`exceptionally` 回调 |
| Broker 模块 | `broker/PopConsumerService.java` | 新增同步 `pop()` / `getMessage()` / `revive()` 方法；`revive` 批量使用 `StructuredTaskScope` |
| Broker 测试 | `broker/PopConsumerServiceTest.java` | `revive()` 返回值从 `CompletableFuture<Boolean>` 改为 `boolean` |
| Broker 测试 | `broker/AckMessageProcessorTest.java` | 新增 `asyncPutMessage` mock，移除不再使用的 `putMessage` stub |
| 代理模块 | `proxy/ProxyConfig.java` | 新增 `enableRemotingVirtualThread` 配置（默认 `true`） |
| 代理模块 | `proxy/RemotingProtocolServer.java` | 6 个 remoting 线程池条件切换为虚拟线程 |

## 三、核心改造点

### 3.1 线程池切换 (BrokerController)

```java
// 改造前：固定大小线程池
sendMessageExecutor = new ThreadPoolExecutor(
    cores, cores, 60s, sendThreadPoolQueue, ...);

// 改造后：虚拟线程（enableVirtualThread=true 时）
sendMessageExecutor = Executors.newVirtualThreadPerTaskExecutor();
```

### 3.2 Send 消息流程简化 (SendMessageProcessor)

```java
// 改造前：CompletableFuture 回调模式
asyncPutMessageFuture.thenAcceptAsync(result -> {
    handlePutMessageResult(result, response, ...);
    doResponse(ctx, request, response);
}, putMessageFutureExecutor);
return null;  // 释放线程，结果在回调中写回

// 改造后：同步风格（虚拟线程 suspend 而非阻塞）
PutMessageResult result = asyncPutMessage(msg).join();
handlePutMessageResult(result, response, ...);
doResponse(ctx, request, response);
return response;
```

### 3.3 ACK 消息流程简化 (AckMessageProcessor)

```java
// 改造前：CompletableFuture 回调模式
asyncPutMessageToSpecificQueue(msgInner).thenAccept(result -> {
    handlePutMessageResult(result, ...);
}).exceptionally(throwable -> {
    handlePutMessageResult(error, ...);
    return null;
});

// 改造后：同步风格（虚拟线程 suspend 而非阻塞）
PutMessageResult result = asyncPutMessageToSpecificQueue(msgInner).join();
handlePutMessageResult(result, ...);
```

### 3.4 Pop 消息 Future 链简化 (PopConsumerService)

```java
// 改造前：多层 thenCompose 链
popAsync() {
    CompletableFuture<PopConsumerContext> future = ...;
    future = getMessageFromTopicAsync(future, ...);
    future = getMessageAsync(future, ...);
    return future.thenCompose(result -> ...)
                 .whenComplete((result, throwable) -> ...);
}

// 改造后：顺序控制流
pop() {
    PopConsumerContext context = ...;
    getMessageFromTopic(context, ...);  // 内部调用 getMessage().join()
    getMessageForQueue(context, ...);
    // 写 records、recode retry message
    return context;
}
```

### 3.5 Revive 并发处理 (PopConsumerService)

```java
// 改造前：CompletableFuture.allOf + Semaphore 手动限流
Semaphore semaphore = new Semaphore(concurrency);
List<CompletableFuture<?>> futureList = new ArrayList<>();
for (record : records) {
    semaphore.acquire();
    futureList.add(revive(record).thenAccept(result -> {
        // handle result
    }).whenComplete((r, ex) -> semaphore.release()));
}
CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();

// 改造后：StructuredTaskScope
try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
    for (record : records) {
        scope.fork(() -> {
            boolean success = this.revive(record);  // 虚拟线程执行
            if (!success) { /* handle failure */ }
            return success;
        });
    }
    scope.join();
    scope.throwIfFailed();
} catch (InterruptedException e) { ... }
```

### 3.6 Proxy Remoting 线程池切换 (RemotingProtocolServer)

```java
// 改造前：固定大小线程池
this.sendMessageExecutor = ThreadPoolMonitor.createAndMonitor(
    config.getRemotingSendMessageThreadPoolNums(), ...);

// 改造后：虚拟线程（enableRemotingVirtualThread=true 时）
this.sendMessageExecutor = ThreadUtils.newVirtualThreadPerTaskExecutor();
// pullMessageExecutor、heartbeatExecutor、updateOffsetExecutor
// topicRouteExecutor、defaultExecutor 同理
```

> **注意：** 字段类型从 `ThreadPoolExecutor` 改为 `ExecutorService`，
> `cleanExpiredRequestInQueue` 对虚拟线程自动跳过队列清理。

## 四、运行环境

### 4.1 编译

```bash
export JAVA_HOME=/path/to/jdk21
mvn clean install -Prelease-all -DskipTests -Dspotbugs.skip=true -Djacoco.skip=true
```

> **注意：** JaCoCo 0.8.5 不支持 JDK 21 class file (major version 65)，需升级 JaCoCo 版本或跳过。

### 4.2 测试

```bash
mvn test -pl broker -am -Dtest=PopConsumerServiceTest -Djacoco.skip=true
# Tests run: 17, Failures: 0, Errors: 0, Skipped: 0
```

### 4.3 启动

```bash
# 启动 Namesrv
export JAVA_HOME=/path/to/jdk21
export ROCKETMQ_HOME=/path/to/rocketmq-distribution
nohup bin/mqnamesrv > logs/namesrv.log 2>&1 &

# 启动 Broker（enableVirtualThread 默认为 true）
nohup bin/mqbroker -n 127.0.0.1:9876 autoCreateTopicEnable=true > logs/broker.log 2>&1 &
```

### 4.4 压测

```bash
# 8 线程并发发送 1000 条消息，全部成功
Total=1000 Success=1000 Fail=0
Time=386ms, TPS=2590
```

## 五、配置项

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `enableVirtualThread` | `true` | 是否启用虚拟线程 |
| `sendMessageThreadPoolNums` | `min(ncpu, 4)` | 虚拟线程关闭时的 send 线程数 |
| `pullMessageThreadPoolNums` | `16 + ncpu * 2` | 虚拟线程关闭时的 pull 线程数 |
| `ackMessageThreadPoolNums` | `16` | 虚拟线程关闭时的 ack 线程数 |
| `enableRemotingVirtualThread` | `true` | Proxy: 是否启用 remoting 虚拟线程 |

关闭虚拟线程（回退到原有模式）：
```properties
enableVirtualThread=false
```

## 六、收益

| 维度 | 改造前 | 改造后 |
|------|--------|--------|
| Send 代码路径 | 3 段分支 (sync / async / vt) | 3 段分支，vt 路径逻辑最简洁 |
| Pop 代码可读性 | 多层 `thenCompose` 嵌套 | 顺序控制流 |
| ACK 代码路径 | `thenAccept` + `exceptionally` 回调 | `.join()` 同步风格 |
| Proxy Remoting 线程数 | 固定大小 (4n*6=24n) | 无上限（虚拟线程按需创建） |
| Revive 并发模型 | `CompletableFuture.allOf` + `Semaphore` | `StructuredTaskScope` |
| 线程调度开销 | 平台线程上下文切换 | 虚拟线程轻量挂起 |
| P99 延迟 | 线程池队列排队 | 虚拟线程自动挂起 |
| 回调线程池 | 需要 `putMessageFutureExecutor` | 虚拟线程不需要 |

## 七、JDK 21 特性使用清单

| 特性 | 使用场景 |
|------|----------|
| **Virtual Threads** | Broker: `sendMessageExecutor`, `pullMessageExecutor`, `ackMessageExecutor`; Proxy: 6 remoting executors; `PopConsumerService.pop()` |
| **StructuredTaskScope** (预览) | `PopConsumerService.revive(AtomicLong, int)` 批量并发处理 |
| **--enable-preview** | 父 pom compiler + surefire 配置 |
