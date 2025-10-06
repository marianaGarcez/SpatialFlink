package GeoFlink.sncb.common;

import org.locationtech.jts.geom.Point;

public class EnrichedEvent {
    public GpsEvent raw;
    public Point ptWgs84;  // EPSG:4326
    public Point ptMetric; // EPSG:25831

    public EnrichedEvent() {}

    public EnrichedEvent(GpsEvent raw, Point ptWgs84, Point ptMetric) {
        this.raw = raw;
        this.ptWgs84 = ptWgs84;
        this.ptMetric = ptMetric;
    }
}

