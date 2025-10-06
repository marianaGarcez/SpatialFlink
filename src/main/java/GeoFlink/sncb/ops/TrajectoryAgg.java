package GeoFlink.sncb.ops;

import GeoFlink.sncb.common.EnrichedEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.locationtech.jts.geom.Coordinate;

import java.util.ArrayList;

public class TrajectoryAgg implements AggregateFunction<EnrichedEvent, TrajectoryAgg.TrajAcc, TrajectoryAgg.TrajAcc> {
    public static class TrajAcc {
        public ArrayList<Coordinate> coords = new ArrayList<>();
    }

    @Override
    public TrajAcc createAccumulator() {
        return new TrajAcc();
    }

    @Override
    public TrajAcc add(EnrichedEvent e, TrajAcc a) {
        a.coords.add(new Coordinate(e.raw.lon, e.raw.lat, e.raw.ts));
        return a;
    }

    @Override
    public TrajAcc getResult(TrajAcc a) {
        return a;
    }

    @Override
    public TrajAcc merge(TrajAcc l, TrajAcc r) {
        l.coords.addAll(r.coords);
        return l;
    }
}

