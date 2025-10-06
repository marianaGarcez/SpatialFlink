package GeoFlink.sncb.mobility;

import GeoFlink.sncb.common.CSVToGpsEventMapFunction;
import GeoFlink.sncb.common.GpsEvent;
import GeoFlink.sncb.ops.TrajSpeedWindowFn;
import GeoFlink.sncb.ops.TrajectoryWindowFn;
import GeoFlink.sncb.ops.VarianceWindowFn;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MobilityRunner {
    public static void main(String[] args) throws Exception {
        String q = argOr(args,0,"q1").toLowerCase();
        String host = argOr(args,1,"host.docker.internal");
        int port = Integer.parseInt(argOr(args,2,"32323"));
        String delim = ",";
        String outDir = argOr(args,3,"/workspace/Output");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // Avoid ClosureCleaner reflection on modern JDKs
        env.getConfig().disableClosureCleaner();

        DataStream<String> lines = env.socketTextStream(host, port, "\n");
        DataStream<GpsEvent> events = lines.map(new CSVToGpsEventMapFunction(delim));

        switch (q) {
            case "q1": {
                DataStream<MN_Q1.CountOut> out = MN_Q1.build(env, events, 4.3658, 50.6456, 2.0);
                out.map(o -> o.start + "," + o.end + "," + o.cnt)
                        .returns(org.apache.flink.api.common.typeinfo.Types.STRING)
                        .writeAsText(outDir + "/output_query1.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
                break; }
            case "q2": {
                DataStream<VarianceWindowFn.VarOut> out = MN_Q2.build(env, events);
                out.map(o -> o.start + "," + o.end + "," + o.varBP + "," + o.varFF + "," + o.cnt)
                        .returns(org.apache.flink.api.common.typeinfo.Types.STRING)
                        .writeAsText(outDir + "/output_query2.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
                break; }
            case "q3": {
                DataStream<TrajectoryWindowFn.TrajOut> out = MN_Q3.build(env, events);
                out.map(o -> o.winStart + "," + o.winEnd + "," + o.wkt)
                        .returns(org.apache.flink.api.common.typeinfo.Types.STRING)
                        .writeAsText(outDir + "/output_query3.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
                break; }
            case "q4": {
                long tMin = java.time.Instant.parse("2024-08-01T00:00:00Z").toEpochMilli();
                long tMax = java.time.Instant.parse("2025-08-01T00:00:00Z").toEpochMilli();
                DataStream<TrajectoryWindowFn.TrajOut> out = MN_Q4.build(env, events, 4.0, 50.0, 4.6, 50.8, tMin, tMax);
                out.map(o -> o.winStart + "," + o.winEnd + "," + o.wkt)
                        .returns(org.apache.flink.api.common.typeinfo.Types.STRING)
                        .writeAsText(outDir + "/output_query4.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
                break; }
            case "q5": {
                double[][] poly = new double[][]{
                        {4.32, 50.60}, {4.32, 50.72}, {4.48, 50.72}, {4.48, 50.60}, {4.32, 50.60}
                };
                DataStream<TrajSpeedWindowFn.TrajSpeedOut> out = MN_Q5.build(env, events, poly, 1.0);
                out.map(o -> o.deviceId + "," + o.winStart + "," + o.winEnd + "," + o.wkt + "," + o.avgSpeed + "," + o.minSpeed)
                        .returns(org.apache.flink.api.common.typeinfo.Types.STRING)
                        .writeAsText(outDir + "/output_query5.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
                break; }
            default: throw new IllegalArgumentException("Unknown query: " + q);
        }

        env.execute("MobilityRunner-" + q);
    }

    private static String argOr(String[] a, int i, String d){ return i<a.length?a[i]:d; }
}
