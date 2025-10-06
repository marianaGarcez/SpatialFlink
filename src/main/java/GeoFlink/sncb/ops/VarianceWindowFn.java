package GeoFlink.sncb.ops;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class VarianceWindowFn extends ProcessWindowFunction<VarianceAgg.VarAcc, VarianceWindowFn.VarOut, String, TimeWindow> {
    public static class VarOut {
        public long start;
        public long end;
        public double varBP; // PCFA
        public double varFF; // PCFF
        public long cnt;
    }

    @Override
    public void process(String key, Context context, Iterable<VarianceAgg.VarAcc> elements, Collector<VarOut> out) {
        VarianceAgg.VarAcc a = elements.iterator().next();
        VarOut v = new VarOut();
        v.start = context.window().getStart();
        v.end = context.window().getEnd();
        v.varBP = VarianceAgg.variance(a.n, a.sumFA, a.sumSqFA);
        v.varFF = VarianceAgg.variance(a.n, a.sumFF, a.sumSqFF);
        v.cnt = a.n;
        out.collect(v);
    }
}

