package com.mn.metrics;

/**
 * Metric name constants for NES-compatible metrics collection.
 * Ensures consistent naming across all operators and queries.
 */
public final class MetricNames {
    // Theoretical throughput (from configuration)
    public static final String THEORETICAL_EPS = "theoretical_eps";
    public static final String THEORETICAL_THROUGHPUT = "theoretical_throughput_mb_s";
    
    // Source metrics (ingress after parsing)
    public static final String SOURCE_IN = "source_in_total";
    
    // Sink metrics (egress)
    public static final String SINK_OUT = "sink_out_total";
    public static final String OUT_BYTES = "out_bytes_total";
    
    // Per-pipeline metrics for selectivity analysis
    public static String pipeIn(String id) { 
        return "pipe_" + id + "_in_total"; 
    }
    
    public static String pipeOut(String id) { 
        return "pipe_" + id + "_out_total"; 
    }
    
    // Latency histogram metrics (defined in FixedBucketLatency)
    public static final String LATENCY_COUNT = "latency_count";
    public static final String LATENCY_SUM = "latency_sum_ms";
    public static final String LATENCY_P50 = "latency_p50_ms";
    public static final String LATENCY_P95 = "latency_p95_ms";
    public static final String LATENCY_P99 = "latency_p99_ms";
    
    private MetricNames() {}
}