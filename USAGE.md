# 快速使用指南

## 1. 编译项目
```bash
mvn clean package -DskipTests
```

## 2. 创建测试 Topic
```bash
# 创建默认 topic (latency-test, 16分区, 3副本)
./create-topic.sh

# 或者指定参数
./create-topic.sh my-topic 8 2
```

## 3. 启动生产者
```bash
# 默认配置：1000 msg/s, 1线程, 10秒统计间隔
./run-producer.sh

# 自定义配置：5000 msg/s, 4线程, 5秒统计间隔
./run-producer.sh latency-test 5000 4 5
```

## 4. 启动消费者（新终端）
```bash
# 默认配置
./run-consumer.sh

# 自定义配置
./run-consumer.sh latency-test my-group 5
```

## 5. 观察输出
生产者和消费者都会定期输出延迟统计信息，包括：
- 消息数量
- 平均延迟
- 最大延迟  
- P99, P99.9, P99.99 延迟

## 6. 停止测试
使用 Ctrl+C 停止生产者和消费者

## 示例命令序列
```bash
# 终端1: 编译和创建topic
mvn clean package -DskipTests
./create-topic.sh

# 终端2: 启动生产者
./run-producer.sh latency-test 2000 2 5

# 终端3: 启动消费者
./run-consumer.sh latency-test test-group 5
```
