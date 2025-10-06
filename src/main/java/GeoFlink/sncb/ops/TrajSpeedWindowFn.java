package GeoFlink.sncb.ops;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.*;

import java.util.Comparator;

public class TrajSpeedWindowFn extends ProcessWindowFunction<TrajSpeedAgg.TrajSpeedAcc, TrajSpeedWindowFn.TrajSpeedOut, String, TimeWindow> {

    public static class TrajSpeedOut {
        public String deviceId;
        public String wkt;
        public double avgSpeed;
        public double minSpeed;
        public long winStart;
        public long winEnd;
    }

    private final GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);

    @Override
    public void process(String key, Context ctx, Iterable<TrajSpeedAgg.TrajSpeedAcc> elements, Collector<TrajSpeedOut> out) {
        TrajSpeedAgg.TrajSpeedAcc a = elements.iterator().next();
        a.coords.sort(Comparator.comparingDouble(c -> c.getZ()));
        
        TrajSpeedOut o = new TrajSpeedOut();
        o.deviceId = key;
        o.avgSpeed = (a.count > 0 ? a.sumSpeed / a.count : 0.0);
        o.minSpeed = (a.count > 0 ? a.minSpeed : Double.NaN);
        o.winStart = ctx.window().getStart();
        o.winEnd = ctx.window().getEnd();
        
        if (a.coords.size() == 0) {
            o.wkt = "POINT EMPTY";
        } else if (a.coords.size() == 1) {
            // Single point - create POINT geometry
            Point pt = gf.createPoint(a.coords.get(0));
            o.wkt = pt.toText();
        } else {
            // Multiple points - create LINESTRING geometry
            LineString ls = gf.createLineString(a.coords.toArray(new Coordinate[0]));
            o.wkt = ls.toText();
        }
        
        out.collect(o);
    }
}

