#!/bin/bash

KAFKA_SERVERS="uaw.msklogstream.oee1gg.c16.kafka.us-east-1.amazonaws.com:9092,boot-eo1.msklogstream.oee1gg.c16.kafka.us-east-1.amazonaws.com:9092,boot-4qw.msklogstream.oee1gg.c16.kafka.us-east-1.amazonaws.com:9092"
TOPIC_NAME=${1:-"latency-test"}
GROUP_ID=${2:-"latency-test-group"}
STATS_INTERVAL=${3:-10}
AUTO_OFFSET_RESET=${4:-"latest"}

echo "Starting consumer:"
echo "  Topic: $TOPIC_NAME"
echo "  Group ID: $GROUP_ID"
echo "  Stats interval: $STATS_INTERVAL seconds"
echo "  Auto offset reset: $AUTO_OFFSET_RESET"

# Extract dependencies if not already extracted
if [ ! -d "BOOT-INF/lib" ]; then
    jar xf target/msk-latency-1.0.0.jar BOOT-INF/lib/
fi

java -cp "target/classes:BOOT-INF/lib/*" com.example.msk.KafkaConsumerApp "$KAFKA_SERVERS" "$TOPIC_NAME" "$GROUP_ID" "$STATS_INTERVAL" "$AUTO_OFFSET_RESET"
