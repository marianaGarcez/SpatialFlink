package com.mn.operators;

import com.mn.metrics.MetricNames;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.Counter;

/**
 * CSV parsing operator that adds monotonic timestamps and tracks source metrics.
 * Mirrors NES's CSVInputFormatter.cpp parsing and SourceThread.cpp timestamping.
 */
public final class CsvParseAndStamp<T> extends RichMapFunction<String, Stamped<T>> {
    private final Parser<T> parser;                // your CSV -> T
    private final long theoreticalRowsPerSec;      // from args
    private final int bytesPerInputRecord;         // constant or from schema

    private Counter sourceIn;

    /**
     * Interface for parsing CSV lines to typed objects.
     */
    public interface Parser<T> { 
        T parse(String line) throws Exception; 
    }

    public CsvParseAndStamp(Parser<T> parser, long theoreticalRowsPerSec, int bytesPerInputRecord) {
        this.parser = parser;
        this.theoreticalRowsPerSec = theoreticalRowsPerSec;
        this.bytesPerInputRecord = bytesPerInputRecord;
    }

    @Override
    public void open(Configuration parameters) {
        MetricGroup g = getRuntimeContext().getMetricGroup();
        this.sourceIn = g.counter(MetricNames.SOURCE_IN);
        
        // Register theoretical throughput gauges
        g.gauge(MetricNames.THEORETICAL_EPS, (Gauge<Long>) () -> theoreticalRowsPerSec);
        g.gauge(MetricNames.THEORETICAL_THROUGHPUT,
                (Gauge<Double>) () -> theoreticalRowsPerSec * (bytesPerInputRecord / 1_000_000.0));
    }

    @Override
    public Stamped<T> map(String line) throws Exception {
        T v = parser.parse(line);              // after parse = "ingress" in NES
        sourceIn.inc();                        // count parsed records
        long ingestNs = System.nanoTime();     // monotonic (NES steady_clock)
        return new Stamped<>(v, ingestNs);
    }
}