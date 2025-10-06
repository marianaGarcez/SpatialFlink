package GeoFlink.sncb.ops;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.*;

import java.util.Comparator;

public class TrajectoryWindowFn extends ProcessWindowFunction<TrajectoryAgg.TrajAcc, TrajectoryWindowFn.TrajOut, String, TimeWindow> {

    public static class TrajOut {
        public String deviceId;
        public String wkt;
        public long winStart;
        public long winEnd;
    }

    private final GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);

    @Override
    public void process(String key, Context ctx, Iterable<TrajectoryAgg.TrajAcc> elements, Collector<TrajOut> out) {
        TrajectoryAgg.TrajAcc a = elements.iterator().next();
        a.coords.sort(Comparator.comparingDouble(c -> c.getZ()));
        
        TrajOut t = new TrajOut();
        t.deviceId = key;
        t.winStart = ctx.window().getStart();
        t.winEnd = ctx.window().getEnd();
        
        if (a.coords.size() == 0) {
            t.wkt = "POINT EMPTY";
        } else if (a.coords.size() == 1) {
            // Single point - create POINT geometry
            Point pt = gf.createPoint(a.coords.get(0));
            t.wkt = pt.toText();
        } else {
            // Multiple points - create LINESTRING geometry
            LineString ls = gf.createLineString(a.coords.toArray(new Coordinate[0]));
            t.wkt = ls.toText();
        }
        
        out.collect(t);
    }
}

