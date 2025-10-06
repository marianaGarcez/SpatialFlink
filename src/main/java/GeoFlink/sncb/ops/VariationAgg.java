package GeoFlink.sncb.ops;

import GeoFlink.sncb.common.EnrichedEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public class VariationAgg implements AggregateFunction<EnrichedEvent, VariationAgg.VarAcc, VariationAgg.VarAcc> {

    public static class VarAcc {
        public double minFA = Double.POSITIVE_INFINITY;
        public double maxFA = Double.NEGATIVE_INFINITY;
        public double minFF = Double.POSITIVE_INFINITY;
        public double maxFF = Double.NEGATIVE_INFINITY;
    }

    @Override
    public VarAcc createAccumulator() {
        return new VarAcc();
    }

    @Override
    public VarAcc add(EnrichedEvent e, VarAcc a) {
        if (e.raw.FA != null) {
            a.minFA = Math.min(a.minFA, e.raw.FA);
            a.maxFA = Math.max(a.maxFA, e.raw.FA);
        }
        if (e.raw.FF != null) {
            a.minFF = Math.min(a.minFF, e.raw.FF);
            a.maxFF = Math.max(a.maxFF, e.raw.FF);
        }
        return a;
    }

    @Override
    public VarAcc getResult(VarAcc a) {
        return a;
    }

    @Override
    public VarAcc merge(VarAcc l, VarAcc r) {
        VarAcc m = new VarAcc();
        m.minFA = Math.min(l.minFA, r.minFA);
        m.maxFA = Math.max(l.maxFA, r.maxFA);
        m.minFF = Math.min(l.minFF, r.minFF);
        m.maxFF = Math.max(l.maxFF, r.maxFF);
        return m;
    }
}

