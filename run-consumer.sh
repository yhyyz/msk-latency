#!/bin/bash

KAFKA_SERVERS="boot-4qw.msklogstream.oee1gg.c16.kafka.us-east-1.amazonaws.com:9092"
KAFKA_SERVERS="b-1.teststandardxlarge.f7cl3f.c3.kafka.us-east-1.amazonaws.com:9092"
KAFKA_SERVERS="boot-4qw.msklogstream.oee1gg.c16.kafka.us-east-1.amazonaws.com:9092,boot-eo1.msklogstream.oee1gg.c16.kafka.us-east-1.amazonaws.com:9092,boot-uaw.msklogstream.oee1gg.c16.kafka.us-east-1.amazonaws.com:9092"
show_help() {
    echo "Kafka Consumer - Latency Test Tool"
    echo ""
    echo "Usage: ./run-consumer.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --topic <name>              Topic name (default: latency-test)"
    echo "  --group <id>                Consumer group ID (default: latency-test-group)"
    echo "  --stats-interval <seconds>  Stats output interval (default: 10)"
    echo "  --offset <earliest|latest>  Auto offset reset (default: latest)"
    echo "  --config <k=v,k=v,...>      Custom Kafka consumer config"
    echo "  --help                      Show this help message"
    echo ""
    echo "Examples:"
    echo "  # Basic usage with defaults"
    echo "  ./run-consumer.sh"
    echo ""
    echo "  # Specify topic and group"
    echo "  ./run-consumer.sh --topic my-topic --group my-group"
    echo ""
    echo "  # Start from beginning"
    echo "  ./run-consumer.sh --topic my-topic --offset earliest"
    echo ""
    echo "  # With custom Kafka config"
    echo "  ./run-consumer.sh --topic my-topic \\"
    echo "    --config \"max.poll.records=1000,fetch.min.bytes=1024\""
    echo ""
    echo "Available --config options:"
    echo "  max.poll.records           Max records per poll (default: 500)"
    echo "  fetch.min.bytes            Min fetch bytes (default: 1)"
    echo "  fetch.max.wait.ms          Max fetch wait time in ms (default: 500)"
    echo "  session.timeout.ms         Session timeout in ms (default: 30000)"
    echo "  heartbeat.interval.ms      Heartbeat interval in ms (default: 3000)"
    echo "  enable.auto.commit         Auto commit enabled (default: true)"
    echo "  auto.commit.interval.ms    Auto commit interval in ms (default: 1000)"
}

if [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    show_help
    exit 0
fi

if [ ! -d "BOOT-INF/lib" ]; then
    jar xf target/msk-latency-1.0.0.jar BOOT-INF/lib/
fi

java -cp "target/classes:BOOT-INF/lib/*" com.example.msk.KafkaConsumerApp --bootstrap-servers "$KAFKA_SERVERS" "$@"
