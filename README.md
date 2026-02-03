# MSK Kafka 延迟测试工具

这是一个用于测试 Amazon MSK (Managed Streaming for Apache Kafka) 延迟性能的 Java 工具集。

## 功能特性

- 创建指定分区数和副本数的 Kafka Topic
- 多线程异步生产者，支持自定义发送速率
- 消费者延迟监控
- 详细的延迟统计（平均值、最大值、P99、P99.9、P99.99）
- **命名参数支持**（`--topic`, `--rate`, `--config` 等）
- **自定义 Kafka 配置参数支持**（通过 `--config` 传递）
- **内置帮助文档**（`--help`）

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
```

## 使用方法

### 查看帮助

```bash
# 生产者帮助
./run-producer.sh --help

# 消费者帮助
./run-consumer.sh --help
```

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
./run-producer.sh --topic my-topic --rate 5000

# 高吞吐量配置
./run-producer.sh --topic my-topic --rate 10000 --threads 4 \
  --config "batch.size=65536,linger.ms=10,compression.type=lz4"

# 发送固定数量的消息
./run-producer.sh --topic my-topic --rate 5000 --total 100000

# 完整参数示例
./run-producer.sh \
  --topic latency-test \
  --rate 5000 \
  --threads 4 \
  --stats-interval 10 \
  --acks 1 \
  --retries 100 \
  --total 100000 \
  --config "batch.size=32768,linger.ms=5"
```

**参数说明：**
| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--topic <name>` | Topic 名称 | latency-test |
| `--rate <n>` | 每秒发送消息数 | 1000 |
| `--threads <n>` | 生产者线程数 | 1 |
| `--stats-interval <sec>` | 统计输出间隔秒数 | 10 |
| `--acks <0\|1\|-1\|all>` | Acks 设置 | -1 |
| `--retries <n>` | 重试次数 | 2147483647 |
| `--total <n>` | 总消息数，-1=无限 | -1 |
| `--config <k=v,...>` | 自定义 Kafka 配置 | - |

### 3. 启动消费者

```bash
# 使用默认参数
./run-consumer.sh

# 指定 topic 和消费者组
./run-consumer.sh --topic my-topic --group my-consumer-group

# 从最早位置开始消费
./run-consumer.sh --topic my-topic --offset earliest

# 使用自定义 Kafka 配置
./run-consumer.sh --topic my-topic \
  --config "max.poll.records=1000,fetch.min.bytes=1024"

# 完整参数示例
./run-consumer.sh \
  --topic latency-test \
  --group my-consumer-group \
  --stats-interval 5 \
  --offset earliest \
  --config "max.poll.records=1000,session.timeout.ms=45000"
```

**参数说明：**
| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--topic <name>` | Topic 名称 | latency-test |
| `--group <id>` | 消费者组 ID | latency-test-group |
| `--stats-interval <sec>` | 统计输出间隔秒数 | 10 |
| `--offset <earliest\|latest>` | 消费起始位置 | latest |
| `--config <k=v,...>` | 自定义 Kafka 配置 | - |

## 输出示例

### 生产者输出
```
Starting producer with:
  --bootstrap-servers  localhost:9092
  --topic              latency-test
  --rate               5000
  --threads            4
  --stats-interval     10 seconds
  --acks               -1
  --retries            2147483647
  --total              unlimited
Custom Kafka config:
  batch.size = 32768
  linger.ms = 5

[2024-01-15 10:30:00.123] Messages: 50000, Latency samples: 50000, Avg: 2.45ms, Max: 15ms, P99: 8ms, P99.9: 12ms, P99.99: 15ms
```

### 消费者输出
```
Starting consumer with:
  --bootstrap-servers  localhost:9092
  --topic              latency-test
  --group              my-consumer-group
  --stats-interval     10 seconds
  --offset             latest
Custom Kafka config:
  max.poll.records = 1000

[2024-01-15 10:30:00.123] Messages: 49998, Latency samples: 49998, Avg: 5.23ms, Max: 25ms, P99: 18ms, P99.9: 22ms, P99.99: 25ms
```

## 自定义 Kafka 配置

通过 `--config` 参数传递自定义 Kafka 配置，格式为 `"key1=value1,key2=value2"`。

自定义配置会**覆盖**默认配置，可以传递任意 Kafka Producer/Consumer 配置项。

### 生产者常用配置

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `batch.size` | 批次大小（字节） | 16384 |
| `linger.ms` | 批次等待时间（毫秒） | 1 |
| `buffer.memory` | 缓冲区大小（字节） | 33554432 |
| `compression.type` | 压缩类型 (none/gzip/snappy/lz4/zstd) | none |
| `max.block.ms` | 最大阻塞时间（毫秒） | 60000 |

**生产者配置示例：**
```bash
# 高吞吐量配置
./run-producer.sh --topic my-topic --rate 10000 \
  --config "batch.size=65536,linger.ms=10,compression.type=lz4"

# 低延迟配置
./run-producer.sh --topic my-topic --rate 1000 \
  --config "batch.size=1,linger.ms=0"
```

### 消费者常用配置

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `max.poll.records` | 单次拉取最大记录数 | 500 |
| `fetch.min.bytes` | 最小拉取字节数 | 1 |
| `fetch.max.wait.ms` | 最大拉取等待时间（毫秒） | 500 |
| `session.timeout.ms` | 会话超时时间（毫秒） | 30000 |
| `heartbeat.interval.ms` | 心跳间隔（毫秒） | 3000 |
| `enable.auto.commit` | 自动提交偏移量 | true |
| `auto.commit.interval.ms` | 自动提交间隔（毫秒） | 1000 |

**消费者配置示例：**
```bash
# 高吞吐量配置
./run-consumer.sh --topic my-topic \
  --config "max.poll.records=1000,fetch.min.bytes=1024"

# 调整会话超时
./run-consumer.sh --topic my-topic \
  --config "session.timeout.ms=45000,heartbeat.interval.ms=15000"
```

## 延迟计算

### 生产者延迟
从调用 `producer.send()` 到收到回调确认的时间差。

### 消费者延迟
从消息中的时间戳到消费者处理时间的差值。消息格式：
```json
{
  "threadId": 0,
  "messageId": 1234,
  "timestamp": 1693574400000,
  "data": "sample-data-0-1234"
}
```

## 性能调优建议

### 提高吞吐量
1. 增加生产者线程数 (`--threads`)
2. 调整 `batch.size` 和 `linger.ms`
3. 启用压缩 (`compression.type=lz4`)
4. 增加分区数以支持更多并行度

### 降低延迟
1. 减少 `linger.ms` 值
2. 使用 `--acks 1` 而不是 `-1`
3. 减小 `batch.size`
4. 调整 JVM 堆大小和 GC 参数

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
