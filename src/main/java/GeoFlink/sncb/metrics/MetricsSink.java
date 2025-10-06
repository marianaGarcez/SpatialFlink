package GeoFlink.sncb.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import java.nio.charset.StandardCharsets;

public class MetricsSink<T> extends RichSinkFunction<T> {
    private final String name;
    private final long sampleIntervalMs;
    private final String outFilePath;
    private final SerializableFunction<T, Integer> sizeFn; // bytes
    private final SerializableFunction<T, Long> latencyBaseTsFn; // returns event ts or window end

    private transient PrintWriter out;
    private transient long lastFlushTime;
    private transient long intervalCount;
    private transient long intervalBytes;
    private transient long intervalLatencySum;
    private transient long intervalLatencyCount;

    public MetricsSink(String name, long sampleIntervalMs, String outFilePath,
                       SerializableFunction<T, Integer> sizeFn,
                       SerializableFunction<T, Long> latencyBaseTsFn) {
        this.name = name;
        this.sampleIntervalMs = sampleIntervalMs;
        this.outFilePath = outFilePath;
        this.sizeFn = sizeFn;
        this.latencyBaseTsFn = latencyBaseTsFn;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        File f = new File(outFilePath);
        File dir = f.getParentFile();
        if (dir != null && !dir.exists()) dir.mkdirs();
        this.out = new PrintWriter(new FileWriter(f, false));
        this.out.println("seconds,count,bytesMB,eps,throughputMBps,avgLatencyMs");
        this.lastFlushTime = System.currentTimeMillis();
        this.intervalCount = 0L;
        this.intervalBytes = 0L;
        this.intervalLatencySum = 0L;
        this.intervalLatencyCount = 0L;
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        long now = System.currentTimeMillis();
        int size = 0;
        if (sizeFn != null) {
            try {
                size = sizeFn.apply(value);
            } catch (Exception ignore) { size = 0; }
        }
        long latency = 0L;
        if (latencyBaseTsFn != null) {
            try {
                Long base = latencyBaseTsFn.apply(value);
                if (base != null && base > 0) latency = Math.max(0L, now - base);
            } catch (Exception ignore) { latency = 0L; }
        }

        intervalCount++;
        intervalBytes += size;
        if (latency > 0) { intervalLatencySum += latency; intervalLatencyCount++; }

        long elapsed = now - lastFlushTime;
        if (elapsed >= sampleIntervalMs) {
            double seconds = elapsed / 1000.0;
            double bytesMB = intervalBytes / (1024.0 * 1024.0);
            double eps = intervalCount / seconds;
            double thrMBps = bytesMB / seconds;
            double avgLat = intervalLatencyCount > 0 ? (intervalLatencySum * 1.0 / intervalLatencyCount) : 0.0;
            out.printf("%.3f,%d,%.6f,%.2f,%.6f,%.3f\n", seconds, intervalCount, bytesMB, eps, thrMBps, avgLat);
            out.flush();

            lastFlushTime = now;
            intervalCount = 0L;
            intervalBytes = 0L;
            intervalLatencySum = 0L;
            intervalLatencyCount = 0L;
        }
    }

    @Override
    public void close() throws IOException {
        if (out != null) {
            out.flush();
            out.close();
        }
    }

    public static int utf8Size(String s) {
        if (s == null) return 0;
        return s.getBytes(StandardCharsets.UTF_8).length;
    }
}
