package GeoFlink.sncb.ops;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class VariationWindowFn extends ProcessWindowFunction<VariationAgg.VarAcc, VariationWindowFn.VarOut, String, TimeWindow> {

    public static class VarOut {
        public String deviceId;
        public double varFA;
        public double varFF;
        public long winStart;
        public long winEnd;
    }

    @Override
    public void process(String key, Context ctx, Iterable<VariationAgg.VarAcc> elements, Collector<VarOut> out) {
        VariationAgg.VarAcc acc = elements.iterator().next();
        VarOut v = new VarOut();
        v.deviceId = key;
        v.varFA = (acc.maxFA - acc.minFA);
        v.varFF = (acc.maxFF - acc.minFF);
        v.winStart = ctx.window().getStart();
        v.winEnd = ctx.window().getEnd();
        out.collect(v);
    }
}

