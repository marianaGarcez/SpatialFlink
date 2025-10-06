package GeoFlink.sncb.mobility;

import GeoFlink.sncb.common.EnrichedEvent;
import GeoFlink.sncb.common.GpsEvent;
import GeoFlink.sncb.ops.TrajSpeedAgg;
import GeoFlink.sncb.ops.TrajSpeedWindowFn;
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

public class MN_Q5 {
    public static DataStream<TrajSpeedWindowFn.TrajSpeedOut> build(StreamExecutionEnvironment env, DataStream<GpsEvent> events,
                                                                   double[][] polyLonLat, double tolMeters) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        // Define grid bounds
        double minX = -180.0, maxX = 180.0, minY = -90.0, maxY = 90.0;
        UniformGrid uGrid = new UniformGrid(100, minX, maxX, minY, maxY);
        
        // Create GeoFlink polygon from coordinates
        List<List<Coordinate>> coordinates = new ArrayList<>();
        List<Coordinate> outerRing = new ArrayList<>();
        for (int i = 0; i < polyLonLat.length; i++) {
            outerRing.add(new Coordinate(polyLonLat[i][0], polyLonLat[i][1]));
        }
        // Close the ring if not already closed
        if (!outerRing.get(0).equals(outerRing.get(outerRing.size() - 1))) {
            outerRing.add(new Coordinate(polyLonLat[0][0], polyLonLat[0][1]));
        }
        coordinates.add(outerRing);
        
        Polygon geoPolygon = new Polygon(coordinates, uGrid);
        
        // Buffer the polygon by tolMeters (approximate buffering)
        org.locationtech.jts.geom.Geometry bufferedPolygon = geoPolygon.polygon.buffer(tolMeters);

        // Convert GPS events to GeoFlink Points and filter by geofence
        DataStream<Point> pointStream = events.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<GpsEvent>(Time.seconds(2)) {
                    @Override public long extractTimestamp(GpsEvent e) { return e.ts; }
                })
                .map(new MapFunction<GpsEvent, Point>() {
                    @Override
                    public Point map(GpsEvent gps) throws Exception {
                        return new Point(gps.deviceId, gps.lon, gps.lat, gps.ts, uGrid);
                    }
                })
                .filter(new FilterFunction<Point>() {
                    @Override
                    public boolean filter(Point point) throws Exception {
                        return bufferedPolygon.contains(point.point);
                    }
                });

        // Convert to enriched events for trajectory and speed calculation
        DataStream<EnrichedEvent> enrichedStream = pointStream.map(new MapFunction<Point, EnrichedEvent>() {
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
                .window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(2)))
                .aggregate(new TrajSpeedAgg(), new TrajSpeedWindowFn())
                .filter(o -> o.avgSpeed < 100.0 || o.minSpeed < 20.0);
    }
}

