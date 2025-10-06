package com.mn.operators;

/**
 * Wrapper class to carry monotonic ingest timestamps with data records.
 * Enables end-to-end latency measurement from source to sink.
 * Mirrors NES's SourceThread.cpp timestamp propagation.
 */
public final class Stamped<T> {
    public final T value;
    public final long ingestNs;  // monotonic timestamp from System.nanoTime()
    
    public Stamped(T value, long ingestNs) {
        this.value = value;
        this.ingestNs = ingestNs;
    }
    
    @Override
    public String toString() {
        return "Stamped{value=" + value + ", ingestNs=" + ingestNs + "}";
    }
}