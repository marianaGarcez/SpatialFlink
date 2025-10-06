package GeoFlink.sncb.mobility;

import GeoFlink.sncb.common.EnrichedEvent;
import GeoFlink.sncb.common.GpsEvent;
import GeoFlink.sncb.ops.VarianceAgg;
import GeoFlink.sncb.ops.VarianceWindowFn;
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

import java.util.ArrayList;
import java.util.List;

public class MN_Q2 {
    public static DataStream<VarianceWindowFn.VarOut> build(StreamExecutionEnvironment env, DataStream<GpsEvent> events) throws Exception {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        // Define grid bounds
        double minX = -180.0, maxX = 180.0, minY = -90.0, maxY = 90.0;
        UniformGrid uGrid = new UniformGrid(100, minX, maxX, minY, maxY);
        
        // Convert GPS events to GeoFlink Points
        DataStream<Point> pointStream = events.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<GpsEvent>(Time.seconds(2)) {
                    @Override public long extractTimestamp(GpsEvent e) { return e.ts; }
                })
                .map(new MapFunction<GpsEvent, Point>() {
                    @Override
                    public Point map(GpsEvent gps) throws Exception {
                        return new Point(gps.deviceId, gps.lon, gps.lat, gps.ts, uGrid);
                    }
                });

        // Create polygon for 4.0..4.6 x 50.0..50.8 using GeoFlink Polygon
        List<List<Coordinate>> coordinates = new ArrayList<>();
        List<Coordinate> outerRing = new ArrayList<>();
        outerRing.add(new Coordinate(4.0, 50.0));
        outerRing.add(new Coordinate(4.0, 50.8));
        outerRing.add(new Coordinate(4.6, 50.8));
        outerRing.add(new Coordinate(4.6, 50.0));
        outerRing.add(new Coordinate(4.0, 50.0)); // Close the ring
        coordinates.add(outerRing);
        
        Polygon excludePolygon = new Polygon(coordinates, uGrid);

        // Filter points that are NOT in the polygon
        DataStream<Point> notInPoly = pointStream.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                // Check if point is NOT inside the polygon
                return !excludePolygon.polygon.intersects(point.point);
            }
        });

        // Convert back to enriched events for the variance calculation
        // Since we need to maintain compatibility with existing variance operators
        DataStream<EnrichedEvent> enrichedStream = notInPoly.map(new MapFunction<Point, EnrichedEvent>() {
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
                .keyBy(e -> "ALL")
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.milliseconds(200)))
                .aggregate(new VarianceAgg(), new VarianceWindowFn());
                // Temporarily remove filter to debug
                //.filter(v -> v.varBP > 0.01 && v.varFF <= 5.0);
    }
}
