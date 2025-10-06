package com.mn.sinks;

import com.mn.metrics.FixedBucketLatency;
import com.mn.metrics.MetricNames;
import com.mn.operators.Stamped;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.ToIntFunction;

/**
 * File sink that measures latency, counts outputs, and tracks bytes.
 * Mirrors NES's FileSink.cpp behavior for result file output.
 */
public final class CountingLatencyFileSink<T> extends RichSinkFunction<Stamped<T>> {
    private final String outputPath;
    private final ToIntFunction<T> sizeEstimator;
    private transient BufferedWriter writer;
    private Counter sinkOut, outBytes;
    private FixedBucketLatency latency;

    public CountingLatencyFileSink(String outputPath, ToIntFunction<T> sizeEstimator) {
        this.outputPath = outputPath;
        this.sizeEstimator = sizeEstimator;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        MetricGroup g = getRuntimeContext().getMetricGroup();
        sinkOut = g.counter(MetricNames.SINK_OUT);
        outBytes = g.counter(MetricNames.OUT_BYTES);
        latency = new FixedBucketLatency(g);
        
        // Open file for writing
        Path path = Paths.get(outputPath);
        path.getParent().toFile().mkdirs(); // Ensure directory exists
        writer = new BufferedWriter(new FileWriter(path.toFile()));
    }

    @Override
    public void invoke(Stamped<T> record, Context ctx) throws IOException {
        long nowNs = System.nanoTime();
        long latencyMs = (nowNs - record.ingestNs) / 1_000_000L;
        
        // Record metrics
        latency.observe(latencyMs);
        sinkOut.inc();
        int bytes = sizeEstimator.applyAsInt(record.value);
        if (bytes > 0) outBytes.inc(bytes);
        
        // Write to file
        writer.write(record.value.toString());
        writer.newLine();
        writer.flush(); // Ensure immediate write for streaming results
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            writer.close();
        }
        super.close();
    }
}