package com.example.msk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SpringBootApplication
public class KafkaConsumerApp implements CommandLineRunner {
    
    private final LatencyStats stats = new LatencyStats();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_TOPIC = "latency-test";
    private static final String DEFAULT_GROUP = "latency-test-group";
    private static final int DEFAULT_STATS_INTERVAL = 10;
    private static final String DEFAULT_OFFSET = "latest";
    
    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerApp.class, args);
    }
    
    private void printHelp() {
        System.out.println("Kafka Consumer - Latency Test Tool");
        System.out.println();
        System.out.println("Usage: java -jar consumer.jar [OPTIONS]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --bootstrap-servers <servers>  Kafka bootstrap servers (default: " + DEFAULT_BOOTSTRAP_SERVERS + ")");
        System.out.println("  --topic <name>                 Topic name (default: " + DEFAULT_TOPIC + ")");
        System.out.println("  --group <id>                   Consumer group ID (default: " + DEFAULT_GROUP + ")");
        System.out.println("  --stats-interval <seconds>     Stats output interval (default: " + DEFAULT_STATS_INTERVAL + ")");
        System.out.println("  --offset <earliest|latest>     Auto offset reset (default: " + DEFAULT_OFFSET + ")");
        System.out.println("  --config <k=v,k=v,...>         Custom Kafka consumer config");
        System.out.println("  --help                         Show this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  # Basic usage");
        System.out.println("  java -jar consumer.jar --bootstrap-servers localhost:9092 --topic my-topic");
        System.out.println();
        System.out.println("  # With custom group and offset");
        System.out.println("  java -jar consumer.jar --topic my-topic --group my-group --offset earliest");
        System.out.println();
        System.out.println("  # With custom Kafka config");
        System.out.println("  java -jar consumer.jar --topic my-topic \\");
        System.out.println("    --config \"max.poll.records=1000,fetch.min.bytes=1024\"");
        System.out.println();
        System.out.println("Available --config options (Kafka Consumer configs):");
        System.out.println("  max.poll.records           Max records per poll (default: 500)");
        System.out.println("  fetch.min.bytes            Min fetch bytes (default: 1)");
        System.out.println("  fetch.max.wait.ms          Max fetch wait time in ms (default: 500)");
        System.out.println("  session.timeout.ms         Session timeout in ms (default: 30000)");
        System.out.println("  heartbeat.interval.ms      Heartbeat interval in ms (default: 3000)");
        System.out.println("  enable.auto.commit         Auto commit enabled (default: true)");
        System.out.println("  auto.commit.interval.ms    Auto commit interval in ms (default: 1000)");
        System.out.println("  ... and any other Kafka consumer configuration");
    }
    
    private Map<String, String> parseArgs(String[] args) {
        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--") && i + 1 < args.length && !args[i + 1].startsWith("--")) {
                params.put(args[i], args[i + 1]);
                i++;
            } else if (args[i].startsWith("--")) {
                params.put(args[i], "");
            }
        }
        return params;
    }
    
    private void applyCustomConfig(Properties props, String configStr) {
        if (configStr == null || configStr.trim().isEmpty()) {
            return;
        }
        System.out.println("Custom Kafka config:");
        for (String pair : configStr.split(",")) {
            String[] kv = pair.split("=", 2);
            if (kv.length == 2) {
                String key = kv[0].trim();
                String value = kv[1].trim();
                props.put(key, value);
                System.out.println("  " + key + " = " + value);
            }
        }
    }
    
    @Override
    public void run(String... args) throws Exception {
        Map<String, String> params = parseArgs(args);
        
        if (params.containsKey("--help") || params.containsKey("-h")) {
            printHelp();
            return;
        }
        
        String bootstrapServers = params.getOrDefault("--bootstrap-servers", DEFAULT_BOOTSTRAP_SERVERS);
        String topic = params.getOrDefault("--topic", DEFAULT_TOPIC);
        String groupId = params.getOrDefault("--group", DEFAULT_GROUP);
        int statsInterval = Integer.parseInt(params.getOrDefault("--stats-interval", String.valueOf(DEFAULT_STATS_INTERVAL)));
        String autoOffsetReset = params.getOrDefault("--offset", DEFAULT_OFFSET);
        String customConfig = params.get("--config");
        
        System.out.println("Starting consumer with:");
        System.out.println("  --bootstrap-servers  " + bootstrapServers);
        System.out.println("  --topic              " + topic);
        System.out.println("  --group              " + groupId);
        System.out.println("  --stats-interval     " + statsInterval + " seconds");
        System.out.println("  --offset             " + autoOffsetReset);
        
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        
        applyCustomConfig(props, customConfig);
        
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        executor.scheduleAtFixedRate(() -> {
            LatencyStats.Stats currentStats = stats.getAndReset();
            System.out.println("[" + sdf.format(new Date()) + "] " + currentStats);
        }, statsInterval, statsInterval, TimeUnit.SECONDS);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            running.set(false);
            executor.shutdown();
        }));
        
        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                long currentTime = System.currentTimeMillis();
                
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JsonNode jsonNode = objectMapper.readTree(record.value());
                        long messageTimestamp = jsonNode.get("timestamp").asLong();
                        long latency = currentTime - messageTimestamp;
                        
                        stats.addLatency(latency);
                    } catch (Exception e) {
                        stats.addMessageCount();
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Consumer error: " + e.getMessage());
        }
    }
}
