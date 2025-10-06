package GeoFlink.sncb.tests;

import GeoFlink.sncb.common.CRSUtils;
import GeoFlink.sncb.common.EnrichedEvent;
import GeoFlink.sncb.common.GpsEvent;
import GeoFlink.sncb.common.PolygonLoader;
import GeoFlink.sncb.metrics.MetricsSink;
import GeoFlink.sncb.ops.TrajSpeedWindowFn;
import GeoFlink.sncb.ops.TrajectoryWindowFn;
import GeoFlink.sncb.ops.VariationWindowFn;
import GeoFlink.sncb.queries.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.prep.PreparedGeometry;

import java.util.List;

public class BenchmarkRunner {
    public static void main(String[] args) throws Exception {
        String query = argOr(args, 0, "q1").toLowerCase();
        long durationSec = Long.parseLong(argOr(args, 1, "30"));
        long targetEps = Long.parseLong(argOr(args, 2, "20000"));

        System.out.printf("Benchmark query=%s duration=%ds targetEps=%d\n", query, durationSec, targetEps);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().disableClosureCleaner();

        // Synthetic source covering Brussels-ish bbox and our sample polygons
        double minLon = 4.30, maxLon = 4.45, minLat = 50.80, maxLat = 50.90;
        DataStream<GpsEvent> events = env.addSource(new SyntheticGpsSource(minLon, maxLon, minLat, maxLat,
                targetEps, durationSec * 1000L, 10))
                .name("synthetic-gps");

        // Source metrics (per-second CSV)
        events.addSink(new MetricsSink<>("source", 1000,
                "metrics/" + query + "_source.csv",
                e -> 0, // theoretical handled offline; we record counts only here
                e -> 0L))
                .name("metrics-source");

        // Prepare static layers
        CRSUtils crs = new CRSUtils();
        PolygonLoader loader = new PolygonLoader(crs);
        List<PreparedGeometry> risk = loader.loadGeoJsonResourceBuffered("high_risk_zones.geojson", 20.0);
        List<PreparedGeometry> maint = loader.loadGeoJsonResourceBuffered("maintenance_areas.geojson", 0.0);
        List<PreparedGeometry> fence = loader.loadWktResourceBuffered("q5_fence.wkt", 1.0);

        // Assign watermarks for event-time windows
        DataStream<GpsEvent> withWm = events.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<GpsEvent>(Time.seconds(2)) {
                    @Override public long extractTimestamp(GpsEvent e) { return e.ts; }
                });

        switch (query) {
            case "q1": {
                DataStream<EnrichedEvent> out = Q1_HighRisk.build(env, withWm, risk);
                out.addSink(new MetricsSink<>("sink-q1", 1000,
                        "metrics/q1_sink.csv",
                        e -> MetricsSink.utf8Size(e.raw.deviceId) + 8 * 3, // approx bytes
                        e -> e.raw.ts)).name("metrics-q1-sink");
                break; }
            case "q2": {
                DataStream<VariationWindowFn.VarOut> out = Q2_BrakeMonitor.build(env, withWm, maint);
                out.addSink(new MetricsSink<>("sink-q2", 1000,
                        "metrics/q2_sink.csv",
                        v -> MetricsSink.utf8Size(v.deviceId) + 8 * 4,
                        v -> v.winEnd)).name("metrics-q2-sink");
                break; }
            case "q3": {
                DataStream<TrajectoryWindowFn.TrajOut> out = Q3_Trajectory.build(env, withWm);
                out.addSink(new MetricsSink<>("sink-q3", 1000,
                        "metrics/q3_sink.csv",
                        t -> MetricsSink.utf8Size(t.deviceId) + MetricsSink.utf8Size(t.wkt),
                        t -> t.winEnd)).name("metrics-q3-sink");
                break; }
            case "q4": {
                long now = System.currentTimeMillis();
                DataStream<TrajectoryWindowFn.TrajOut> out = Q4_TrajectoryRestricted.build(env, withWm,
                        4.3, 4.5, 50.0, 50.6, now - 3600_000L, now + 3600_000L);
                out.addSink(new MetricsSink<>("sink-q4", 1000,
                        "metrics/q4_sink.csv",
                        t -> MetricsSink.utf8Size(t.deviceId) + MetricsSink.utf8Size(t.wkt),
                        t -> t.winEnd)).name("metrics-q4-sink");
                break; }
            case "q5": {
                DataStream<TrajSpeedWindowFn.TrajSpeedOut> out = Q5_TrajAndSpeedFence.build(env, withWm, fence, 50.0, 20.0);
                out.addSink(new MetricsSink<>("sink-q5", 1000,
                        "metrics/q5_sink.csv",
                        t -> MetricsSink.utf8Size(t.deviceId) + MetricsSink.utf8Size(t.wkt) + 8 * 2,
                        t -> t.winEnd)).name("metrics-q5-sink");
                break; }
            default: throw new IllegalArgumentException("Unknown query: " + query);
        }

        env.execute("BenchmarkRunner-" + query);
    }

    private static String argOr(String[] args, int i, String d) { return i < args.length ? args[i] : d; }
}
