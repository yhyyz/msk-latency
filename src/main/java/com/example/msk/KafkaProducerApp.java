package com.example.msk;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.text.SimpleDateFormat;
import java.util.Date;
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
    
    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerApp.class, args);
    }
    
    @Override
    public void run(String... args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java -jar producer.jar <bootstrap-servers> <topic> [messages-per-second] [threads] [stats-interval-seconds] [acks] [retries] [total-messages]");
            return;
        }
        
        String bootstrapServers = args[0];
        String topic = args[1];
        int messagesPerSecond = args.length > 2 ? Integer.parseInt(args[2]) : 1000;
        int threads = args.length > 3 ? Integer.parseInt(args[3]) : 1;
        int statsInterval = args.length > 4 ? Integer.parseInt(args[4]) : 10;
        String acks = args.length > 5 ? args[5] : "-1";
        int retries = args.length > 6 ? Integer.parseInt(args[6]) : Integer.MAX_VALUE;
        long totalMessages = args.length > 7 ? Long.parseLong(args[7]) : -1;
        
        System.out.println("Starting producer with:");
        System.out.println("Bootstrap servers: " + bootstrapServers);
        System.out.println("Topic: " + topic);
        System.out.println("Messages per second: " + messagesPerSecond);
        System.out.println("Threads: " + threads);
        System.out.println("Stats interval: " + statsInterval + " seconds");
        System.out.println("Acks: " + acks);
        System.out.println("Retries: " + retries);
        if (totalMessages > 0) {
            System.out.println("Total messages: " + totalMessages);
        } else {
            System.out.println("Total messages: unlimited");
        }
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.RETRIES_CONFIG, retries);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(threads + 1);
        
        // Stats reporting
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        executor.scheduleAtFixedRate(() -> {
            LatencyStats.Stats currentStats = stats.getAndReset();
            System.out.println("[" + sdf.format(new Date()) + "] " + currentStats);
        }, statsInterval, statsInterval, TimeUnit.SECONDS);
        
        // Producer threads
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
                        // Check if we've reached the total message limit
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
                                    
                                    // Check if we've reached the total and stop (print only once)
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
                            Thread.sleep(0, 100000); // Sleep 0.1ms
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
        
        // Keep main thread alive
        while (running.get()) {
            Thread.sleep(1000);
        }
    }
    
    private String createMessage(int threadId, int messageId, long timestamp) {
        return String.format("{\"threadId\":%d,\"messageId\":%d,\"timestamp\":%d,\"data\":\"sample-data-%d-%d\"}", 
                threadId, messageId, timestamp, threadId, messageId);
    }
}
