package com.example.msk;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@SpringBootApplication
public class KafkaProducerApp implements CommandLineRunner {
    
    private final LatencyStats stats = new LatencyStats();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong messageCounter = new AtomicLong(0);
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private volatile long startTime;
    
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_TOPIC = "latency-test";
    private static final int DEFAULT_RATE = 1000;
    private static final int DEFAULT_THREADS = 1;
    private static final int DEFAULT_STATS_INTERVAL = 10;
    private static final String DEFAULT_ACKS = "-1";
    private static final int DEFAULT_RETRIES = Integer.MAX_VALUE;
    private static final long DEFAULT_TOTAL = -1;
    
    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerApp.class, args);
    }
    
    private void printHelp() {
        System.out.println("Kafka Producer - Latency Test Tool");
        System.out.println();
        System.out.println("Usage: java -jar producer.jar [OPTIONS]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --bootstrap-servers <servers>  Kafka bootstrap servers (default: " + DEFAULT_BOOTSTRAP_SERVERS + ")");
        System.out.println("  --topic <name>                 Topic name (default: " + DEFAULT_TOPIC + ")");
        System.out.println("  --rate <n>                     Messages per second (default: " + DEFAULT_RATE + ")");
        System.out.println("  --threads <n>                  Producer threads (default: " + DEFAULT_THREADS + ")");
        System.out.println("  --stats-interval <seconds>     Stats output interval (default: " + DEFAULT_STATS_INTERVAL + ")");
        System.out.println("  --acks <0|1|-1|all>            Acks setting (default: " + DEFAULT_ACKS + ")");
        System.out.println("  --retries <n>                  Retry count (default: " + DEFAULT_RETRIES + ")");
        System.out.println("  --total <n>                    Total messages, -1=unlimited (default: " + DEFAULT_TOTAL + ")");
        System.out.println("  --config <k=v,k=v,...>         Custom Kafka producer config");
        System.out.println("  --help                         Show this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  # Basic usage");
        System.out.println("  java -jar producer.jar --bootstrap-servers localhost:9092 --topic my-topic");
        System.out.println();
        System.out.println("  # High throughput with custom config");
        System.out.println("  java -jar producer.jar --topic my-topic --rate 10000 --threads 4 \\");
        System.out.println("    --config \"batch.size=65536,linger.ms=10,compression.type=lz4\"");
        System.out.println();
        System.out.println("  # Send fixed number of messages");
        System.out.println("  java -jar producer.jar --topic my-topic --rate 5000 --total 100000");
        System.out.println();
        System.out.println("Available --config options (Kafka Producer configs):");
        System.out.println("  batch.size          Batch size in bytes (default: 16384)");
        System.out.println("  linger.ms           Batch wait time in ms (default: 1)");
        System.out.println("  buffer.memory       Buffer memory in bytes (default: 33554432)");
        System.out.println("  compression.type    none, gzip, snappy, lz4, zstd (default: none)");
        System.out.println("  max.block.ms        Max block time in ms (default: 60000)");
        System.out.println("  ... and any other Kafka producer configuration");
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
        int messagesPerSecond = Integer.parseInt(params.getOrDefault("--rate", String.valueOf(DEFAULT_RATE)));
        int threads = Integer.parseInt(params.getOrDefault("--threads", String.valueOf(DEFAULT_THREADS)));
        int statsInterval = Integer.parseInt(params.getOrDefault("--stats-interval", String.valueOf(DEFAULT_STATS_INTERVAL)));
        String acks = params.getOrDefault("--acks", DEFAULT_ACKS);
        int retries = Integer.parseInt(params.getOrDefault("--retries", String.valueOf(DEFAULT_RETRIES)));
        long totalMessages = Long.parseLong(params.getOrDefault("--total", String.valueOf(DEFAULT_TOTAL)));
        String customConfig = params.get("--config");
        
        System.out.println("Starting producer with:");
        System.out.println("  --bootstrap-servers  " + bootstrapServers);
        System.out.println("  --topic              " + topic);
        System.out.println("  --rate               " + messagesPerSecond);
        System.out.println("  --threads            " + threads);
        System.out.println("  --stats-interval     " + statsInterval + " seconds");
        System.out.println("  --acks               " + acks);
        System.out.println("  --retries            " + retries);
        System.out.println("  --total              " + (totalMessages > 0 ? totalMessages : "unlimited"));
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.RETRIES_CONFIG, retries);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        
        applyCustomConfig(props, customConfig);
        
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(threads + 1);
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        executor.scheduleAtFixedRate(() -> {
            LatencyStats.Stats currentStats = stats.getAndReset();
            System.out.println("[" + sdf.format(new Date()) + "] " + currentStats);
        }, statsInterval, statsInterval, TimeUnit.SECONDS);
        
        int messagesPerThread = messagesPerSecond / threads;
        long intervalNanos = 1_000_000_000L / messagesPerThread;
        
        startTime = System.currentTimeMillis();
        
        for (int i = 0; i < threads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try (Producer<String, String> producer = new KafkaProducer<>(props)) {
                    long nextSendTime = System.nanoTime();
                    int messageId = 0;
                    
                    while (running.get()) {
                        if (totalMessages > 0 && messageCounter.get() >= totalMessages) {
                            break;
                        }
                        
                        long currentTime = System.nanoTime();
                        if (currentTime >= nextSendTime) {
                            String key = "thread-" + threadId + "-msg-" + messageId++;
                            String value = createMessage(threadId, messageId, System.currentTimeMillis());
                            
                            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                            final long sendTime = System.currentTimeMillis();
                            
                            producer.send(record, (metadata, exception) -> {
                                long latency = System.currentTimeMillis() - sendTime;
                                if (exception == null) {
                                    stats.addLatency(latency);
                                    long count = messageCounter.incrementAndGet();
                                    
                                    if (totalMessages > 0 && count >= totalMessages && completed.compareAndSet(false, true)) {
                                        long endTime = System.currentTimeMillis();
                                        long totalTime = endTime - startTime;
                                        System.out.println("\n=== COMPLETED ===");
                                        System.out.println("Total messages sent: " + count);
                                        System.out.println("Total time: " + totalTime + "ms (" + (totalTime / 1000.0) + "s)");
                                        System.out.println("Average throughput: " + (count * 1000.0 / totalTime) + " messages/second");
                                        running.set(false);
                                    }
                                } else {
                                    System.err.println("Send failed: " + exception.getMessage());
                                }
                            });
                            
                            nextSendTime += intervalNanos;
                        } else {
                            Thread.sleep(0, 100000);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Producer thread " + threadId + " error: " + e.getMessage());
                }
            });
        }
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            running.set(false);
            executor.shutdown();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        
        while (running.get()) {
            Thread.sleep(1000);
        }
    }
    
    private String createMessage(int threadId, int messageId, long timestamp) {
        return String.format("{\"threadId\":%d,\"messageId\":%d,\"timestamp\":%d,\"data\":\"sample-data-%d-%d\"}", 
                threadId, messageId, timestamp, threadId, messageId);
    }
}
