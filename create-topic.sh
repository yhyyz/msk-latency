#!/bin/bash

KAFKA_SERVERS="uaw.msklogstream.oee1gg.c16.kafka.us-east-1.amazonaws.com:9092,boot-eo1.msklogstream.oee1gg.c16.kafka.us-east-1.amazonaws.com:9092,boot-4qw.msklogstream.oee1gg.c16.kafka.us-east-1.amazonaws.com:9092"
TOPIC_NAME=${1:-"latency-test"}
PARTITIONS=${2:-16}
REPLICATION_FACTOR=${3:-3}

echo "Creating topic: $TOPIC_NAME with $PARTITIONS partitions and replication factor $REPLICATION_FACTOR"

java -cp "target/classes:target/lib/*" com.example.msk.TopicCreator "$KAFKA_SERVERS" "$TOPIC_NAME" "$PARTITIONS" "$REPLICATION_FACTOR"
