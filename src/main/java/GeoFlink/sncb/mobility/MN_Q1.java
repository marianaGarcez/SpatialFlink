package GeoFlink.sncb.mobility;

import GeoFlink.sncb.common.GpsEvent;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.spatialOperators.range.PointPointRangeQuery;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashSet;

public class MN_Q1 {
    public static class CountOut { 
        public long start; 
        public long end; 
        public long cnt; 
        public CountOut() {}
        public CountOut(long start, long end, long cnt) {
            this.start = start;
            this.end = end;
            this.cnt = cnt;
        }
    }

    public static DataStream<CountOut> build(StreamExecutionEnvironment env, DataStream<GpsEvent> events,
                                             double lon, double lat, double tolMeters) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        // Define grid bounds - you may need to adjust these based on your data coverage
        double minX = -180.0, maxX = 180.0, minY = -90.0, maxY = 90.0;
        UniformGrid uGrid = new UniformGrid(100, minX, maxX, minY, maxY);  // 100-unit grid cells
        
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

        // Create query point
        Point queryPoint = new Point("query", lon, lat, 0L, uGrid);
        HashSet<Point> queryPoints = new HashSet<>();
        queryPoints.add(queryPoint);

        // Configure GeoFlink range query
        QueryConfiguration qConfig = new QueryConfiguration(QueryType.RealTime);
        PointPointRangeQuery rangeQuery = new PointPointRangeQuery(qConfig, uGrid);
        
        // Execute range query to find points within tolMeters
        DataStream<Point> withinRange = rangeQuery.run(pointStream, queryPoints, tolMeters);

        // Window the results and count
        AllWindowedStream<Point, TimeWindow> win = withinRange.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));
        return win.process(new ProcessAllWindowFunction<Point, CountOut, TimeWindow>() {
            @Override
            public void process(Context ctx, Iterable<Point> elements, Collector<CountOut> out) {
                long c = 0; 
                for (Point p : elements) c++;
                CountOut o = new CountOut(ctx.window().getStart(), ctx.window().getEnd(), c);
                out.collect(o);
            }
        });
    }
}
