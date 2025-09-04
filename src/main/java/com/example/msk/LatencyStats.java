package com.example.msk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class LatencyStats {
    private final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong messageCount = new AtomicLong(0);
    private final AtomicLong totalLatency = new AtomicLong(0);
    private volatile long maxLatency = 0;
    
    public void addLatency(long latency) {
        latencies.add(latency);
        messageCount.incrementAndGet();
        totalLatency.addAndGet(latency);
        if (latency > maxLatency) {
            maxLatency = latency;
        }
    }
    
    public void addMessageCount() {
        messageCount.incrementAndGet();
    }
    
    public synchronized Stats getAndReset() {
        if (latencies.isEmpty()) {
            return new Stats(0, 0, 0, 0, 0, 0, 0);
        }
        
        List<Long> sortedLatencies = new ArrayList<>(latencies);
        Collections.sort(sortedLatencies);
        
        long count = messageCount.get();
        double avgLatency = count > 0 ? (double) totalLatency.get() / count : 0;
        long max = maxLatency;
        long p99 = getPercentile(sortedLatencies, 99.0);
        long p999 = getPercentile(sortedLatencies, 99.9);
        long p9999 = getPercentile(sortedLatencies, 99.99);
        
        // Reset
        latencies.clear();
        messageCount.set(0);
        totalLatency.set(0);
        maxLatency = 0;
        
        return new Stats(count, avgLatency, max, p99, p999, p9999, sortedLatencies.size());
    }
    
    private long getPercentile(List<Long> sortedLatencies, double percentile) {
        if (sortedLatencies.isEmpty()) return 0;
        int index = (int) Math.ceil(percentile / 100.0 * sortedLatencies.size()) - 1;
        index = Math.max(0, Math.min(index, sortedLatencies.size() - 1));
        return sortedLatencies.get(index);
    }
    
    public static class Stats {
        public final long messageCount;
        public final double avgLatency;
        public final long maxLatency;
        public final long p99;
        public final long p999;
        public final long p9999;
        public final long latencyCount;
        
        public Stats(long messageCount, double avgLatency, long maxLatency, long p99, long p999, long p9999, long latencyCount) {
            this.messageCount = messageCount;
            this.avgLatency = avgLatency;
            this.maxLatency = maxLatency;
            this.p99 = p99;
            this.p999 = p999;
            this.p9999 = p9999;
            this.latencyCount = latencyCount;
        }
        
        @Override
        public String toString() {
            return String.format("Messages: %d, Latency samples: %d, Avg: %.2fms, Max: %dms, P99: %dms, P99.9: %dms, P99.99: %dms",
                    messageCount, latencyCount, avgLatency, maxLatency, p99, p999, p9999);
        }
    }
}
