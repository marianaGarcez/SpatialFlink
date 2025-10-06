package GeoFlink.sncb.queries;

import GeoFlink.sncb.common.*;
import GeoFlink.sncb.ops.VariationAgg;
import GeoFlink.sncb.ops.VariationWindowFn;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.prep.PreparedGeometry;

import java.util.ArrayList;
import java.util.List;

public class Q2_BrakeMonitor {

    public static DataStream<VariationWindowFn.VarOut> build(StreamExecutionEnvironment env, DataStream<GpsEvent> events,
                                                             List<PreparedGeometry> maintenanceAreas) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Define grid bounds
        double minX = -180.0, maxX = 180.0, minY = -90.0, maxY = 90.0;
        UniformGrid uGrid = new UniformGrid(100, minX, maxX, minY, maxY);

        // Convert GPS events to GeoFlink Points
        DataStream<Point> pointStream = events.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<GpsEvent>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(GpsEvent e) { return e.ts; }
                })
                .map(new MapFunction<GpsEvent, Point>() {
                    @Override
                    public Point map(GpsEvent gps) throws Exception {
                        return new Point(gps.deviceId, gps.lon, gps.lat, gps.ts, uGrid);
                    }
                });

        // Convert PreparedGeometry maintenance areas to GeoFlink Polygons
        List<Polygon> maintenancePolygons = new ArrayList<>();
        for (PreparedGeometry pg : maintenanceAreas) {
            org.locationtech.jts.geom.Geometry geom = pg.getGeometry();
            if (geom instanceof org.locationtech.jts.geom.Polygon) {
                org.locationtech.jts.geom.Polygon jtsPolygon = (org.locationtech.jts.geom.Polygon) geom;
                
                // Convert JTS polygon to GeoFlink polygon format
                List<List<Coordinate>> coordinates = new ArrayList<>();
                List<Coordinate> outerRing = new ArrayList<>();
                Coordinate[] coords = jtsPolygon.getExteriorRing().getCoordinates();
                for (Coordinate coord : coords) {
                    outerRing.add(coord);
                }
                coordinates.add(outerRing);
                
                Polygon geoPolygon = new Polygon(coordinates, uGrid);
                maintenancePolygons.add(geoPolygon);
            }
        }

        // Filter out points that are in maintenance areas
        DataStream<Point> notInMaint = pointStream.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                // Check if point is NOT in any maintenance area
                for (Polygon maintenancePoly : maintenancePolygons) {
                    if (maintenancePoly.polygon.intersects(point.point)) {
                        return false; // Point is in maintenance area, exclude it
                    }
                }
                return true; // Point is not in any maintenance area, include it
            }
        });

        // Convert back to EnrichedEvent format for variance calculation
        DataStream<EnrichedEvent> enrichedStream = notInMaint.map(new MapFunction<Point, EnrichedEvent>() {
            @Override
            public EnrichedEvent map(Point p) throws Exception {
                GpsEvent gps = new GpsEvent();
                gps.deviceId = p.objID;
                gps.ts = p.timeStampMillisec;
                gps.lon = p.point.getX();
                gps.lat = p.point.getY();
                
                EnrichedEvent enriched = new EnrichedEvent();
                enriched.raw = gps;
                enriched.ptWgs84 = p.point;
                return enriched;
            }
        });

        return enrichedStream
                .keyBy(e -> e.raw.deviceId)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.milliseconds(10)))
                .aggregate(new VariationAgg(), new VariationWindowFn())
                .filter(v -> v.varFA > 0.6 && v.varFF <= 0.5);
    }
}
