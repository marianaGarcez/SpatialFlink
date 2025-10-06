package com.mn.metrics.reporter;

import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NES-compatible metrics reporter that writes periodic snapshots every 5 seconds.
 * Mirrors NES's StatisticPrinter.cpp behavior for EngineStats_*.stats output.
 */
public final class NESFileReporter implements MetricReporter, Scheduled {
    // track only the counters we care about; key is a fully-scoped name per subtask
    private final Map<String, Long> last = new ConcurrentHashMap<>();
    private final Map<String, Metric> metrics = new ConcurrentHashMap<>();
    private Path outDir;
    private String jobIdOrQueryId = "Q-unknown";

    @Override
    public void open(MetricConfig config) {
        String dir = config.getString("outDir", "/tmp/nes-metrics");
        outDir = Paths.get(dir);
        try { 
            Files.createDirectories(outDir); 
        } catch (IOException e) {
            System.err.println("Warning: Could not create metrics output directory: " + dir);
        }
        jobIdOrQueryId = config.getString("queryId", jobIdOrQueryId);
        System.out.println("NESFileReporter initialized: outDir=" + outDir + ", queryId=" + jobIdOrQueryId);
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, org.apache.flink.metrics.MetricGroup group) {
        // Use simple key based on metric name for now
        String key = metricName + "_" + System.identityHashCode(group);
        metrics.put(key, metric);
    }

    @Override 
    public void notifyOfRemovedMetric(Metric metric, String name, org.apache.flink.metrics.MetricGroup group) {
        String key = name + "_" + System.identityHashCode(group);
        metrics.remove(key);
        last.remove(key);
    }

    @Override
    public void report() {
        Instant now = Instant.now();
        long nowMs = System.currentTimeMillis();

        // Build one "METRICS" line per subprocess (TaskManager/JobManager)
        StringBuilder sb = new StringBuilder();
        sb.append("METRICS ts=").append(now).append(' ');

        long deltaSource = 0, deltaSink = 0, deltaBytes = 0;

        for (Map.Entry<String, Metric> e : metrics.entrySet()) {
            String k = e.getKey();
            Metric m = e.getValue();
            if (isCounter(m)) {
                long v = getCounterValue(m);
                long prev = last.getOrDefault(k, v);
                long delta = v - prev;
                last.put(k, v);

                if (k.endsWith("source_in_total")) deltaSource += delta;
                if (k.endsWith("sink_out_total"))  deltaSink   += delta;
                if (k.endsWith("out_bytes_total"))  deltaBytes += delta;
            }
        }
        
        // 5s is configured via flink-conf; derive EPS and selectivity from deltas and interval
        double intervalSec = 5.0;
        double epsIn  = deltaSource / intervalSec;
        double epsOut = deltaSink   / intervalSec;
        double sel    = (deltaSource > 0) ? ((double) deltaSink / (double) deltaSource) : Double.NaN;
        double mbps   = (deltaBytes / intervalSec) / 1_000_000.0;

        sb.append("eps_in_avg=").append(String.format("%.2f", epsIn))
          .append(" eps_out_avg=").append(String.format("%.2f", epsOut))
          .append(" selectivity_e2e=").append(String.format("%.4f", sel))
          .append(" throughput_mb_s=").append(String.format("%.4f", mbps));

        // write a line; one file per process
        Path stats = outDir.resolve("EngineStats_" + jobIdOrQueryId + "_proc.stats");
        try (BufferedWriter w = Files.newBufferedWriter(stats, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            w.write(sb.toString());
            w.newLine();
        } catch (IOException e) {
            System.err.println("Warning: Could not write metrics to " + stats + ": " + e.getMessage());
        }
    }

    @Override
    public void close() {
        // Per-process close. Job-level rollup is done by the external combiner.
        System.out.println("NESFileReporter closed for query: " + jobIdOrQueryId);
    }

    private static boolean isCounter(Metric m) {
        try { 
            m.getClass().getMethod("getCount"); 
            return true; 
        } catch (NoSuchMethodException e) { 
            return false; 
        }
    }
    
    private static long getCounterValue(Metric m) {
        try { 
            return (long) m.getClass().getMethod("getCount").invoke(m); 
        } catch (Exception e) { 
            return 0L; 
        }
    }
}