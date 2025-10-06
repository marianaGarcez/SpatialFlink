package com.mn.operators;

import com.mn.metrics.MetricNames;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

/**
 * Map operator that counts input/output records for per-pipeline selectivity analysis.
 * Mirrors NES's CompiledExecutablePipelineStage.cpp counting behavior.
 */
public final class CountingMap<T> extends RichMapFunction<T, T> {
    private final String pipeId;
    private Counter in, out;

    public CountingMap(String pipeId) { 
        this.pipeId = pipeId; 
    }

    @Override
    public void open(Configuration parameters) {
        MetricGroup g = getRuntimeContext().getMetricGroup();
        in = g.counter(MetricNames.pipeIn(pipeId));
        out = g.counter(MetricNames.pipeOut(pipeId));
    }

    @Override
    public T map(T value) {
        in.inc();
        out.inc();     // increments when the record is emitted
        return value;
    }
}