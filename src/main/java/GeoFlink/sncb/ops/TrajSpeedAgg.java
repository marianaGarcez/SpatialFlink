package GeoFlink.sncb.ops;

import GeoFlink.sncb.common.EnrichedEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.locationtech.jts.geom.Coordinate;

import java.util.ArrayList;

public class TrajSpeedAgg implements AggregateFunction<EnrichedEvent, TrajSpeedAgg.TrajSpeedAcc, TrajSpeedAgg.TrajSpeedAcc> {
    public static class TrajSpeedAcc {
        public ArrayList<Coordinate> coords = new ArrayList<>();
        public double sumSpeed = 0d;
        public long count = 0L;
        public double minSpeed = Double.POSITIVE_INFINITY;
    }

    @Override
    public TrajSpeedAcc createAccumulator() { return new TrajSpeedAcc(); }

    @Override
    public TrajSpeedAcc add(EnrichedEvent e, TrajSpeedAcc a) {
        a.coords.add(new Coordinate(e.raw.lon, e.raw.lat, e.raw.ts));
        if (e.raw.gpsSpeed != null) {
            a.sumSpeed += e.raw.gpsSpeed;
            a.count++;
            a.minSpeed = Math.min(a.minSpeed, e.raw.gpsSpeed);
        }
        return a;
    }

    @Override
    public TrajSpeedAcc getResult(TrajSpeedAcc a) { return a; }

    @Override
    public TrajSpeedAcc merge(TrajSpeedAcc l, TrajSpeedAcc r) {
        l.coords.addAll(r.coords);
        l.sumSpeed += r.sumSpeed;
        l.count += r.count;
        l.minSpeed = Math.min(l.minSpeed, r.minSpeed);
        return l;
    }
}

