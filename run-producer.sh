#!/bin/bash

KAFKA_SERVERS="boot-4qw.msklogstream.oee1gg.c16.kafka.us-east-1.amazonaws.com:9092"

show_help() {
    echo "Kafka Producer - Latency Test Tool"
    echo ""
    echo "Usage: ./run-producer.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --topic <name>              Topic name (default: latency-test)"
    echo "  --rate <n>                  Messages per second (default: 1000)"
    echo "  --threads <n>               Producer threads (default: 1)"
    echo "  --stats-interval <seconds>  Stats output interval (default: 10)"
    echo "  --acks <0|1|-1|all>         Acks setting (default: -1)"
    echo "  --retries <n>               Retry count (default: 2147483647)"
    echo "  --total <n>                 Total messages, -1=unlimited (default: -1)"
    echo "  --config <k=v,k=v,...>      Custom Kafka producer config"
    echo "  --help                      Show this help message"
    echo ""
    echo "Examples:"
    echo "  # Basic usage with defaults"
    echo "  ./run-producer.sh"
    echo ""
    echo "  # Specify topic and rate"
    echo "  ./run-producer.sh --topic my-topic --rate 5000"
    echo ""
    echo "  # High throughput with custom config"
    echo "  ./run-producer.sh --topic my-topic --rate 10000 --threads 4 \\"
    echo "    --config \"batch.size=65536,linger.ms=10,compression.type=lz4\""
    echo ""
    echo "  # Send fixed number of messages"
    echo "  ./run-producer.sh --topic my-topic --rate 5000 --total 100000"
    echo ""
    echo "Available --config options:"
    echo "  batch.size          Batch size in bytes (default: 16384)"
    echo "  linger.ms           Batch wait time in ms (default: 1)"
    echo "  buffer.memory       Buffer memory in bytes (default: 33554432)"
    echo "  compression.type    none, gzip, snappy, lz4, zstd (default: none)"
    echo "  max.block.ms        Max block time in ms (default: 60000)"
}

if [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    show_help
    exit 0
fi

java -jar target/msk-latency-1.0.0.jar --bootstrap-servers "$KAFKA_SERVERS" "$@"
