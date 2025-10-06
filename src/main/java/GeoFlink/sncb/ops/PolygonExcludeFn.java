package GeoFlink.sncb.ops;

import GeoFlink.sncb.common.EnrichedEvent;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.prep.PreparedGeometry;

import java.util.Collections;
import java.util.List;

public class PolygonExcludeFn extends BroadcastProcessFunction<EnrichedEvent, List<PreparedGeometry>, EnrichedEvent> {
    private volatile List<PreparedGeometry> geoms = Collections.emptyList();

    @Override
    public void processBroadcastElement(List<PreparedGeometry> value, Context ctx, Collector<EnrichedEvent> out) {
        geoms = value;
    }

    @Override
    public void processElement(EnrichedEvent ev, ReadOnlyContext ctx, Collector<EnrichedEvent> out) {
        for (PreparedGeometry g : geoms) {
            if (g.intersects(ev.ptMetric)) {
                return; // drop this event
            }
        }
        out.collect(ev);
    }
}

