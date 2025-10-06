package GeoFlink.sncb.mobility;

import GeoFlink.sncb.common.EnrichedEvent;
import GeoFlink.sncb.common.GpsEvent;
import GeoFlink.sncb.ops.TrajectoryAgg;
import GeoFlink.sncb.ops.TrajectoryWindowFn;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;

public class MN_Q4 {
    public static DataStream<TrajectoryWindowFn.TrajOut> build(StreamExecutionEnvironment env, DataStream<GpsEvent> events,
                                                               double minLon, double minLat, double maxLon, double maxLat,
                                                               long tMin, long tMax) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        // Define grid bounds
        double gridMinX = -180.0, gridMaxX = 180.0, gridMinY = -90.0, gridMaxY = 90.0;
        UniformGrid uGrid = new UniformGrid(100, gridMinX, gridMaxX, gridMinY, gridMaxY);
        
        // Apply temporal and spatial filtering early, then convert to GeoFlink Points
        DataStream<Point> pointStream = events.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<GpsEvent>(Time.seconds(2)) {
                    @Override public long extractTimestamp(GpsEvent e) { return e.ts; }
                })
                .filter(new FilterFunction<GpsEvent>() {
                    @Override
                    public boolean filter(GpsEvent e) throws Exception {
                        return e.lon >= minLon && e.lon <= maxLon && 
                               e.lat >= minLat && e.lat <= maxLat && 
                               e.ts >= tMin && e.ts <= tMax;
                    }
                })
                .map(new MapFunction<GpsEvent, Point>() {
                    @Override
                    public Point map(GpsEvent gps) throws Exception {
                        return new Point(gps.deviceId, gps.lon, gps.lat, gps.ts, uGrid);
                    }
                });

        // Convert to enriched events for trajectory calculation
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
                .keyBy(e -> "ALL")
                .window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(2)))
                .aggregate(new TrajectoryAgg(), new TrajectoryWindowFn());
    }
}

