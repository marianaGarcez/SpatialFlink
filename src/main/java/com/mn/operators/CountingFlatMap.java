package com.mn.operators;

import com.mn.metrics.MetricNames;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;

/**
 * FlatMap operator that counts input/output records for per-pipeline selectivity analysis.
 * Useful for operators that can produce 0, 1, or many outputs per input.
 */
public final class CountingFlatMap<T, O> extends RichFlatMapFunction<T, O> {
    private final String pipeId;
    private final FlatMapFunction<T, O> function;
    private Counter in, out;

    /**
     * Interface for the actual flat map logic.
     */
    public interface FlatMapFunction<T, O> {
        void flatMap(T value, Collector<O> out) throws Exception;
    }

    public CountingFlatMap(String pipeId, FlatMapFunction<T, O> function) {
        this.pipeId = pipeId;
        this.function = function;
    }

    @Override
    public void open(Configuration parameters) {
        MetricGroup g = getRuntimeContext().getMetricGroup();
        in = g.counter(MetricNames.pipeIn(pipeId));
        out = g.counter(MetricNames.pipeOut(pipeId));
    }

    @Override
    public void flatMap(T value, Collector<O> collector) throws Exception {
        in.inc();
        
        // Use a counting collector to track outputs
        CountingCollector<O> countingCollector = new CountingCollector<>(collector, out);
        function.flatMap(value, countingCollector);
    }

    /**
     * Collector wrapper that counts emitted records.
     */
    private static class CountingCollector<O> implements Collector<O> {
        private final Collector<O> wrapped;
        private final Counter outCounter;

        CountingCollector(Collector<O> wrapped, Counter outCounter) {
            this.wrapped = wrapped;
            this.outCounter = outCounter;
        }

        @Override
        public void collect(O record) {
            outCounter.inc();
            wrapped.collect(record);
        }

        @Override
        public void close() {
            wrapped.close();
        }
    }
}