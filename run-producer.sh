#!/bin/bash

KAFKA_SERVERS="xxxx:9092"
TOPIC_NAME=${1:-"latency-test"}
MESSAGES_PER_SECOND=${2:-1000}
THREADS=${3:-1}
STATS_INTERVAL=${4:-10}
ACKS=${5:-"-1"}
RETRIES=${6:-2147483647}
TOTAL_MESSAGES=${7:-"-1"}

echo "Starting producer:"
echo "  Topic: $TOPIC_NAME"
echo "  Messages per second: $MESSAGES_PER_SECOND"
echo "  Threads: $THREADS"
echo "  Stats interval: $STATS_INTERVAL seconds"
echo "  Acks: $ACKS"
echo "  Retries: $RETRIES"
if [ "$TOTAL_MESSAGES" != "-1" ]; then
    echo "  Total messages: $TOTAL_MESSAGES"
else
    echo "  Total messages: unlimited"
fi

java -jar target/msk-latency-1.0.0.jar "$KAFKA_SERVERS" "$TOPIC_NAME" "$MESSAGES_PER_SECOND" "$THREADS" "$STATS_INTERVAL" "$ACKS" "$RETRIES" "$TOTAL_MESSAGES"
