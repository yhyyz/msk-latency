#!/bin/bash

KAFKA_SERVERS="b-1.teststandardxlarge.f7cl3f.c3.kafka.us-east-1.amazonaws.com:9092"
TOPIC_NAME=${1:-"latency-test"}
PARTITIONS=${2:-16}
REPLICATION_FACTOR=${3:-3}

echo "Creating topic: $TOPIC_NAME with $PARTITIONS partitions and replication factor $REPLICATION_FACTOR"

java -cp "target/classes:target/lib/*" com.example.msk.TopicCreator "$KAFKA_SERVERS" "$TOPIC_NAME" "$PARTITIONS" "$REPLICATION_FACTOR"
