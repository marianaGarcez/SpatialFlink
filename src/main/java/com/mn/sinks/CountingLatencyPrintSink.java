package com.mn.sinks;

import com.mn.metrics.FixedBucketLatency;
import com.mn.metrics.MetricNames;
import com.mn.operators.Stamped;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.function.ToIntFunction;

/**
 * Sink that measures latency, counts outputs, and tracks bytes.
 * Mirrors NES's PrintSink.cpp and FileSink.cpp behavior.
 */
public final class CountingLatencyPrintSink<T> extends RichSinkFunction<Stamped<T>> {
    private final ToIntFunction<T> sizeEstimator; // serialized bytes; return 0 if unknown
    private Counter sinkOut, outBytes;
    private FixedBucketLatency latency;

    public CountingLatencyPrintSink(ToIntFunction<T> sizeEstimator) {
        this.sizeEstimator = sizeEstimator;
    }

    @Override
    public void open(Configuration parameters) {
        MetricGroup g = getRuntimeContext().getMetricGroup();
        sinkOut = g.counter(MetricNames.SINK_OUT);
        outBytes = g.counter(MetricNames.OUT_BYTES);
        latency = new FixedBucketLatency(g);
    }

    @Override
    public void invoke(Stamped<T> record, Context ctx) {
        long nowNs = System.nanoTime();
        long latencyMs = (nowNs - record.ingestNs) / 1_000_000L;
        
        // Record metrics
        latency.observe(latencyMs);
        sinkOut.inc();
        int bytes = sizeEstimator.applyAsInt(record.value);
        if (bytes > 0) outBytes.inc(bytes);
        
        // Print output (replace with real sink write as needed)
        System.out.println(record.value);
    }
}