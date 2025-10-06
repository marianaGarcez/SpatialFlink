package GeoFlink.sncb.queries;

import GeoFlink.sncb.common.*;
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

public class Q3_Trajectory {
    public static DataStream<TrajectoryWindowFn.TrajOut> build(StreamExecutionEnvironment env, DataStream<GpsEvent> events) {
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

        // Convert back to EnrichedEvent format for trajectory calculation
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
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.milliseconds(10)))
                .aggregate(new TrajectoryAgg(), new TrajectoryWindowFn());
    }
}
