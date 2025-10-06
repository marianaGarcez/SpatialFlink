package GeoFlink.sncb.ops;

import GeoFlink.sncb.common.EnrichedEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public class VarianceAgg implements AggregateFunction<EnrichedEvent, VarianceAgg.VarAcc, VarianceAgg.VarAcc> {
    public static class VarAcc {
        public long n = 0L;
        public double sumFA = 0d, sumSqFA = 0d;
        public double sumFF = 0d, sumSqFF = 0d;
    }

    @Override
    public VarAcc createAccumulator() { return new VarAcc(); }

    @Override
    public VarAcc add(EnrichedEvent e, VarAcc a) {
        if (e.raw.FA != null) { a.sumFA += e.raw.FA; a.sumSqFA += e.raw.FA * e.raw.FA; }
        if (e.raw.FF != null) { a.sumFF += e.raw.FF; a.sumSqFF += e.raw.FF * e.raw.FF; }
        a.n++;
        return a;
    }

    @Override
    public VarAcc getResult(VarAcc a) { return a; }

    @Override
    public VarAcc merge(VarAcc l, VarAcc r) {
        VarAcc m = new VarAcc();
        m.n = l.n + r.n;
        m.sumFA = l.sumFA + r.sumFA; m.sumSqFA = l.sumSqFA + r.sumSqFA;
        m.sumFF = l.sumFF + r.sumFF; m.sumSqFF = l.sumSqFF + r.sumSqFF;
        return m;
    }

    public static double variance(long n, double sum, double sumSq) {
        if (n <= 1) return 0.0;
        double mean = sum / n;
        double var = (sumSq / n) - (mean * mean);
        return Math.max(0.0, var);
    }
}

