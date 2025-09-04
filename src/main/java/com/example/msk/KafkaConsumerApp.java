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
    
    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerApp.class, args);
    }
    
    @Override
    public void run(String... args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: java -jar consumer.jar <bootstrap-servers> <topic> <group-id> [stats-interval-seconds] [auto-offset-reset]");
            return;
        }
        
        String bootstrapServers = args[0];
        String topic = args[1];
        String groupId = args[2];
        int statsInterval = args.length > 3 ? Integer.parseInt(args[3]) : 10;
        String autoOffsetReset = args.length > 4 ? args[4] : "latest";
        
        System.out.println("Starting consumer with:");
        System.out.println("Bootstrap servers: " + bootstrapServers);
        System.out.println("Topic: " + topic);
        System.out.println("Group ID: " + groupId);
        System.out.println("Stats interval: " + statsInterval + " seconds");
        System.out.println("Auto offset reset: " + autoOffsetReset);
        
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
        
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        
        // Stats reporting
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
                        long messageTimestamp = jsonNode.get("timestamp").asLong(); // Already in millis
                        long latency = currentTime - messageTimestamp;
                        
                        stats.addLatency(latency);
                    } catch (Exception e) {
                        // If parsing fails, just count the message
                        stats.addMessageCount();
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Consumer error: " + e.getMessage());
        }
    }
}
