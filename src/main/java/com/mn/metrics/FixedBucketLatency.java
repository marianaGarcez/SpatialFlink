package com.mn.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Fixed-bucket latency histogram matching NES bucket boundaries.
 * Provides exact compatibility with MobilityNebula metrics collection.
 */
public final class FixedBucketLatency {
    // NES buckets in milliseconds (upper bounds, inclusive semantics via "le" naming)
    public static final long[] BUCKETS_MS =
        {0,1,2,4,8,16,32,64,128,256,512,1000,2000,5000,10000,20000,60000};

    private final Counter[] bucketCounters; // registered counters (so reporters can export raw)
    private final Counter sampleCount;
    private final Counter sumMs;
    private final AtomicLongArray shadow;   // readable local copy for gauges

    public FixedBucketLatency(MetricGroup group) {
        this.bucketCounters = new Counter[BUCKETS_MS.length];
        for (int i = 0; i < BUCKETS_MS.length; i++) {
            bucketCounters[i] = group.counter("latency_bucket_le_" + BUCKETS_MS[i]);
        }
        this.sampleCount = group.counter("latency_count");
        this.sumMs = group.counter("latency_sum_ms");
        this.shadow = new AtomicLongArray(BUCKETS_MS.length);

        // Percentile gauges computed from counters
        group.gauge("latency_p50_ms", (Gauge<Double>) () -> percentile(0.50));
        group.gauge("latency_p95_ms", (Gauge<Double>) () -> percentile(0.95));
        group.gauge("latency_p99_ms", (Gauge<Double>) () -> percentile(0.99));
    }

    /**
     * Record a latency observation in the histogram.
     * @param latencyMs latency in milliseconds
     */
    public void observe(long latencyMs) {
        int idx = Arrays.binarySearch(BUCKETS_MS, latencyMs);
        if (idx < 0) idx = Math.min(BUCKETS_MS.length - 1, -idx - 1);
        bucketCounters[idx].inc();
        shadow.addAndGet(idx, 1);
        sampleCount.inc();
        sumMs.inc(latencyMs);
    }

    /**
     * Compute percentile from the histogram buckets.
     * @param p percentile (0.0 to 1.0)
     * @return percentile value in milliseconds
     */
    private double percentile(double p) {
        long total = sampleCount.getCount();
        if (total <= 0) return Double.NaN;
        long rank = (long)Math.ceil(p * total);
        long cum = 0;
        for (int i = 0; i < BUCKETS_MS.length; i++) {
            cum += shadow.get(i);
            if (cum >= rank) return (double) BUCKETS_MS[i];
        }
        return BUCKETS_MS[BUCKETS_MS.length - 1];
    }
}