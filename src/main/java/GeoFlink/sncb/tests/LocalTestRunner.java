package GeoFlink.sncb.tests;

import GeoFlink.sncb.common.*;
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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class LocalTestRunner {

    public static void main(String[] args) throws Exception {
        String which = (args.length > 0 ? args[0].toLowerCase() : "q1");
        System.out.println("Running local test: " + which);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().disableClosureCleaner();

        List<GpsEvent> data = sampleData();

        DataStream<GpsEvent> src = env.fromCollection(data)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<GpsEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(GpsEvent e) { return e.ts; }
                });

        CRSUtils crs = new CRSUtils();
        PolygonLoader loader = new PolygonLoader(crs);

        switch (which) {
            case "q1": {
                List<PreparedGeometry> risk = loader.loadGeoJsonResourceBuffered("high_risk_zones.geojson", 20.0);
                Q1_HighRisk.build(env, src, risk)
                        .map(e -> "device=" + e.raw.deviceId + ", ts=" + e.raw.ts + ", lon=" + e.raw.lon + ", lat=" + e.raw.lat)
                        .returns(String.class)
                        .print("Q1");
                break; }
            case "q2": {
                List<PreparedGeometry> maint = loader.loadGeoJsonResourceBuffered("maintenance_areas.geojson", 0.0);
                Q2_BrakeMonitor.build(env, src, maint)
                        .map(v -> "device=" + v.deviceId + ", varFA=" + v.varFA + ", varFF=" + v.varFF + ", window=[" + v.winStart + "," + v.winEnd + "]")
                        .returns(String.class)
                        .print("Q2");
                break; }
            case "q3": {
                DataStream<TrajectoryWindowFn.TrajOut> out = Q3_Trajectory.build(env, src);
                out.map(t -> "device=" + t.deviceId + ", wkt=" + t.wkt)
                        .returns(String.class)
                        .print("Q3");
                break; }
            case "q4": {
                long base = Instant.parse("2024-10-24T00:00:00Z").toEpochMilli();
                long tMin = base;
                long tMax = base + 86_400_000L; // +1 day
                DataStream<TrajectoryWindowFn.TrajOut> out = Q4_TrajectoryRestricted.build(env, src,
                        4.3, 4.5, 50.0, 50.6, tMin, tMax);
                out.map(t -> "device=" + t.deviceId + ", wkt=" + t.wkt)
                        .returns(String.class)
                        .print("Q4");
                break; }
            case "q5": {
                List<PreparedGeometry> fence = loader.loadWktResourceBuffered("q5_fence.wkt", 1.0);
                DataStream<TrajSpeedWindowFn.TrajSpeedOut> out = Q5_TrajAndSpeedFence.build(env, src, fence, 50.0, 20.0);
                out.map(o -> "device=" + o.deviceId + ", avgSpeed=" + o.avgSpeed + ", minSpeed=" + o.minSpeed + ", wkt=" + o.wkt)
                        .returns(String.class)
                        .print("Q5");
                break; }
            default:
                throw new IllegalArgumentException("Unknown query: " + which);
        }

        env.execute("LocalTestRunner " + which);
    }

    private static List<GpsEvent> sampleData() {
        List<GpsEvent> list = new ArrayList<>();
        long t0 = System.currentTimeMillis();

        // Q1: points near 4.35..4.36, 50.85..50.86 (inside high_risk_zones polygon)
        list.add(new GpsEvent("A", 4.352, 50.852, t0 + 1000, 10.0, 0.1, 0.1));
        list.add(new GpsEvent("A", 4.355, 50.855, t0 + 2000, 11.0, 0.2, 0.2));
        list.add(new GpsEvent("A", 4.358, 50.858, t0 + 3000, 12.0, 0.8, 0.4));

        // Q2: outside maintenance area (which is 4.38..4.39), make variation FA>0.6, FF<=0.5
        list.add(new GpsEvent("B", 4.370, 50.852, t0 + 1100, 8.0, 0.1, 0.5));
        list.add(new GpsEvent("B", 4.372, 50.853, t0 + 2100, 8.5, 0.8, 0.4));
        list.add(new GpsEvent("B", 4.374, 50.854, t0 + 3100, 9.0, 0.7, 0.3));

        // Q3/Q4: two devices, build simple trajectories within bbox 4.3..4.5, 50.0..50.6
        list.add(new GpsEvent("C", 4.40, 50.10, t0 + 1200, 15.0, null, null));
        list.add(new GpsEvent("C", 4.41, 50.11, t0 + 2200, 15.5, null, null));
        list.add(new GpsEvent("C", 4.42, 50.12, t0 + 3200, 16.0, null, null));

        list.add(new GpsEvent("D", 4.31, 50.20, t0 + 1300, 17.0, null, null));
        list.add(new GpsEvent("D", 4.33, 50.22, t0 + 2300, 18.0, null, null));
        list.add(new GpsEvent("D", 4.35, 50.24, t0 + 3300, 19.0, null, null));

        // Q5: inside fence 4.4..4.41 & speed avg > 50, min > 20
        list.add(new GpsEvent("E", 4.405, 50.855, t0 + 1400, 60.0, null, null));
        list.add(new GpsEvent("E", 4.406, 50.856, t0 + 2400, 55.0, null, null));
        list.add(new GpsEvent("E", 4.407, 50.857, t0 + 3400, 40.0, null, null));

        return list;
    }
}
