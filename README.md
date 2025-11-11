# MSK Kafka 延迟测试工具

这是一个用于测试 Amazon MSK (Managed Streaming for Apache Kafka) 延迟性能的 Java 工具集。

## 功能特性

- 创建指定分区数和副本数的 Kafka Topic
- 多线程异步生产者，支持自定义发送速率
- 消费者延迟监控
- 详细的延迟统计（平均值、最大值、P99、P99.9、P99.99）
- 命令行参数配置

## 项目结构

```
msk-latency/
├── src/main/java/com/example/msk/
│   ├── TopicCreator.java          # Topic 创建工具
│   ├── LatencyStats.java          # 延迟统计工具类
│   ├── KafkaProducerApp.java      # 生产者应用
│   └── KafkaConsumerApp.java      # 消费者应用
├── create-topic.sh                # Topic 创建脚本
├── run-producer.sh               # 生产者启动脚本
├── run-consumer.sh               # 消费者启动脚本
├── pom.xml                       # Maven 配置
└── README.md                     # 使用文档
```

## 编译项目

```bash
mvn clean package -DskipTests
mvn dependency:copy-dependencies -DoutputDirectory=target/lib
# 编译好的
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/msk-latency-1.0.0.jar
```

## 使用方法

### 1. 创建 Topic

```bash
# 使用默认参数（16分区，3副本）
./create-topic.sh

# 指定 topic 名称
./create-topic.sh my-test-topic

# 指定所有参数
./create-topic.sh my-test-topic 32 3
```

**参数说明：**
- 参数1: Topic 名称（默认: latency-test）
- 参数2: 分区数（默认: 16）
- 参数3: 副本数（默认: 3）

### 2. 启动生产者

```bash
# 使用默认参数
./run-producer.sh

# 指定 topic 和发送速率
./run-producer.sh latency-test 5000

# 指定所有参数
./run-producer.sh latency-test 5000 4 10 1 100 10000
```

**参数说明：**
- 参数1: Topic 名称（默认: latency-test）
- 参数2: 每秒发送消息数（默认: 1000）
- 参数3: 生产者线程数（默认: 1）
- 参数4: 统计输出间隔秒数（默认: 10）
- 参数5: Acks 设置（默认: -1，可选: 0, 1, -1/all）
- 参数6: 重试次数（默认: 2147483647，即 Integer.MAX_VALUE）
- 参数7: 总消息数（默认: -1 无限制，设置正数则发送指定条数后停止）

### 3. 启动消费者

```bash
# 使用默认参数
./run-consumer.sh

# 指定 topic 和消费者组
./run-consumer.sh latency-test my-consumer-group

# 指定所有参数
./run-consumer.sh latency-test my-consumer-group 5 earliest
```

**参数说明：**
- 参数1: Topic 名称（默认: latency-test）
- 参数2: 消费者组 ID（默认: latency-test-group）
- 参数3: 统计输出间隔秒数（默认: 10）
- 参数4: 消费位置（默认: latest，可选: earliest, latest）

## 输出示例

### 生产者输出
```
Starting producer:
  Topic: latency-test
  Messages per second: 1000
  Threads: 1
  Stats interval: 10 seconds

[1693574400000] Messages: 10000, Latency samples: 10000, Avg: 2.45ms, Max: 15ms, P99: 8ms, P99.9: 12ms, P99.99: 15ms
```

### 消费者输出
```
Starting consumer:
  Topic: latency-test
  Group ID: latency-test-group
  Stats interval: 10 seconds

[1693574400000] Messages: 9998, Latency samples: 9998, Avg: 5.23ms, Max: 25ms, P99: 18ms, P99.9: 22ms, P99.99: 25ms
```

## 配置说明

### Kafka 集群配置


### 生产者配置
- `acks=1`: 等待 leader 确认
- `retries=3`: 重试次数
- `batch.size=16384`: 批次大小
- `linger.ms=1`: 批次等待时间
- `buffer.memory=33554432`: 缓冲区大小

### 消费者配置
- `auto.offset.reset=latest`: 从最新位置开始消费
- `enable.auto.commit=true`: 自动提交偏移量
- `session.timeout.ms=30000`: 会话超时时间
- `max.poll.records=500`: 单次拉取最大记录数

## 延迟计算

### 生产者延迟
从调用 `producer.send()` 到收到回调确认的时间差。

### 消费者延迟
从消息中的时间戳到消费者处理时间的差值。消息格式：
```json
{
  "threadId": 0,
  "messageId": 1234,
  "timestamp": 1693574400000000000,
  "data": "sample-data-0-1234"
}
```

## 性能调优建议

### 提高吞吐量
1. 增加生产者线程数
2. 调整 `batch.size` 和 `linger.ms`
3. 增加分区数以支持更多并行度

### 降低延迟
1. 减少 `linger.ms` 值
2. 使用 `acks=1` 而不是 `acks=all`
3. 调整 JVM 堆大小和 GC 参数

### 监控指标
- 消息发送速率 (messages/second)
- 平均延迟 (ms)
- P99 延迟 (ms)
- 错误率

## 故障排除

### 常见问题

1. **连接失败**
   - 检查网络连接和安全组配置
   - 确认 MSK 集群地址正确

2. **权限错误**
   - 检查 IAM 角色和策略
   - 确认 MSK 集群访问控制配置

3. **性能问题**
   - 监控 JVM 内存使用
   - 检查网络带宽限制
   - 调整 Kafka 客户端配置

### 日志级别调整
在 `application.properties` 中调整日志级别：
```properties
logging.level.org.apache.kafka=DEBUG
logging.level.com.example.msk=DEBUG
```

## 依赖版本

- Java: 11+
- Spring Boot: 2.7.0
- Kafka Client: 3.1.0
- Jackson: 2.13.3

