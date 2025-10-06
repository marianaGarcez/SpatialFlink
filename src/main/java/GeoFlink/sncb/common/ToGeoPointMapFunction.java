package GeoFlink.sncb.common;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.locationtech.jts.geom.Point;

public class ToGeoPointMapFunction extends RichMapFunction<GpsEvent, EnrichedEvent> {
    private transient CRSUtils crs;

    public ToGeoPointMapFunction() {}

    @Override
    public void open(Configuration parameters) {
        this.crs = new CRSUtils();
    }

    @Override
    public EnrichedEvent map(GpsEvent e) {
        Point wgs = crs.pointWgs84(e.lon, e.lat);
        Point metric = crs.toMetric(wgs);
        return new EnrichedEvent(e, wgs, metric);
    }
}
