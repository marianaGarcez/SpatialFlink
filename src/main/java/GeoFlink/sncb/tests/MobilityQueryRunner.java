package GeoFlink.sncb.tests;

import GeoFlink.sncb.common.GpsEvent;
import GeoFlink.sncb.mobility.*;
import GeoFlink.sncb.ops.TrajSpeedWindowFn;
import GeoFlink.sncb.ops.TrajectoryWindowFn;
import GeoFlink.sncb.ops.VarianceWindowFn;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test runner for all Mobility Queries (MN_Q1-Q5) with statistics collection
 */
public class MobilityQueryRunner {

    private static final String OUTPUT_DIR = "metrics/";
    private static final AtomicLong resultCounter = new AtomicLong(0);
    private static final List<String> executionStats = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        String queryToRun = (args.length > 0 ? args[0].toLowerCase() : "all");
        System.out.println("Running Mobility Query Test: " + queryToRun);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().disableClosureCleaner();

        // Create file-based source for real data as a more reliable alternative to TCP
        DataStream<GpsEvent> sourceStream = env.readTextFile("Data/selected_columns_df.csv")
                .map(new MapFunction<String, GpsEvent>() {
                    @Override
                    public GpsEvent map(String csvLine) throws Exception {
                        String[] fields = csvLine.split(",");
                        if (fields.length < 14) {
                            throw new RuntimeException("Invalid CSV line: " + csvLine);
                        }
                        
                        // Parse CSV: time_utc,device_id,Vbat,PCFA_mbar,PCFF_mbar,PCF1_mbar,PCF2_mbar,T1_mbar,T2_mbar,Code1,Code2,gps_speed,gps_lat,gps_lon
                        long timestamp = Long.parseLong(fields[0].trim()) * 1000L; // Convert to milliseconds
                        String deviceId = fields[1].trim();
                        double bp = Double.parseDouble(fields[3].trim()); // PCFA_mbar as brake pressure
                        double ff = Double.parseDouble(fields[4].trim()); // PCFF_mbar as fuel flow
                        double lat = Double.parseDouble(fields[12].trim()); // gps_lat
                        double lon = Double.parseDouble(fields[13].trim()); // gps_lon
                        double speed = Double.parseDouble(fields[11].trim()); // gps_speed
                        
                        // Debug: Print first few events to verify timestamp parsing
                        if (Math.random() < 0.01) { // Print ~1% of events to avoid spam
                            System.out.println("DEBUG: Parsed timestamp=" + timestamp + ", deviceId=" + deviceId + ", bp=" + bp + ", ff=" + ff);
                        }
                        
                        return new GpsEvent(deviceId, lon, lat, timestamp, speed, bp, ff);
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<GpsEvent>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(GpsEvent e) { return e.ts; }
                });

        long startTime = System.currentTimeMillis();

        switch (queryToRun) {
            case "mn_q1":
            case "q1":
                runMN_Q1(env, sourceStream);
                break;
            case "mn_q2":
            case "q2":
                runMN_Q2(env, sourceStream);
                break;
            case "mn_q3":
            case "q3":
                runMN_Q3(env, sourceStream);
                break;
            case "mn_q4":
            case "q4":
                runMN_Q4(env, sourceStream);
                break;
            case "mn_q5":
            case "q5":
                runMN_Q5(env, sourceStream);
                break;
            case "all":
            default:
                runAllQueries(env, sourceStream);
                break;
        }

        env.execute("MobilityQueryRunner_" + queryToRun);
        
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        
        // Generate statistics report
        generateStatisticsReport(queryToRun, executionTime);
    }

    private static void runMN_Q1(StreamExecutionEnvironment env, DataStream<GpsEvent> sourceStream) throws Exception {
        System.out.println("=== Running MN_Q1: Point-in-Circle Proximity Query ===");
        
        // Target point: Brussels area
        double targetLon = 4.35;
        double targetLat = 50.85;
        double toleranceMeters = 100.0;
        
        DataStream<MN_Q1.CountOut> results = MN_Q1.build(env, sourceStream, targetLon, targetLat, toleranceMeters);
        
        // Format and save results
        DataStream<String> formattedResults = results.map(new MapFunction<MN_Q1.CountOut, String>() {
            @Override
            public String map(MN_Q1.CountOut result) throws Exception {
                return String.format("MN_Q1: count=%d, window=[%d-%d]", 
                    result.cnt, result.start, result.end);
            }
        });
        
        formattedResults.addSink(new StringResultCollectorSink("MN_Q1", "Point-in-Circle Proximity"))
                       .name("MN_Q1_Sink");
               
        formattedResults.print("MN_Q1_Output");
    }

    private static void runMN_Q2(StreamExecutionEnvironment env, DataStream<GpsEvent> sourceStream) throws Exception {
        System.out.println("=== Running MN_Q2: Variance Analysis with Spatial Exclusion ===");
        
        DataStream<VarianceWindowFn.VarOut> results = MN_Q2.build(env, sourceStream);
        
        // Format and save results
        DataStream<String> formattedResults = results.map(new MapFunction<VarianceWindowFn.VarOut, String>() {
            @Override
            public String map(VarianceWindowFn.VarOut result) throws Exception {
                return String.format("MN_Q2: varBP=%.3f, varFF=%.3f, window=[%d-%d]", 
                    result.varBP, result.varFF, result.start, result.end);
            }
        });
        
        formattedResults.addSink(new StringResultCollectorSink("MN_Q2", "Variance Analysis"))
                       .name("MN_Q2_Sink");
               
        formattedResults.print("MN_Q2_Output");
    }

    private static void runMN_Q3(StreamExecutionEnvironment env, DataStream<GpsEvent> sourceStream) throws Exception {
        System.out.println("=== Running MN_Q3: Global Trajectory Analysis ===");
        
        DataStream<TrajectoryWindowFn.TrajOut> results = MN_Q3.build(env, sourceStream);
        
        // Format and save results
        DataStream<String> formattedResults = results.map(new MapFunction<TrajectoryWindowFn.TrajOut, String>() {
            @Override
            public String map(TrajectoryWindowFn.TrajOut result) throws Exception {
                return String.format("MN_Q3: device=%s, wkt=%s, window=[%d-%d]", 
                    result.deviceId, result.wkt, result.winStart, result.winEnd);
            }
        });
        
        formattedResults.addSink(new StringResultCollectorSink("MN_Q3", "Global Trajectory"))
                       .name("MN_Q3_Sink");
               
        formattedResults.print("MN_Q3_Output");
    }

    private static void runMN_Q4(StreamExecutionEnvironment env, DataStream<GpsEvent> sourceStream) throws Exception {
        System.out.println("=== Running MN_Q4: Spatially/Temporally Bounded Trajectory ===");
        
        // Bounding box for Brussels region
        double minLon = 4.3, maxLon = 4.5;
        double minLat = 50.0, maxLat = 51.0;
        
        // Use wider time bounds to capture the real data (August 2024)
        // Real data timestamps are around 1722520348 (epoch seconds)
        long tMin = 1722520000L * 1000L; // Convert to milliseconds  
        long tMax = 1722530000L * 1000L; // About 3 hours of data
        
        DataStream<TrajectoryWindowFn.TrajOut> results = MN_Q4.build(env, sourceStream, 
            minLon, minLat, maxLon, maxLat, tMin, tMax);
        
        // Format and save results
        DataStream<String> formattedResults = results.map(new MapFunction<TrajectoryWindowFn.TrajOut, String>() {
            @Override
            public String map(TrajectoryWindowFn.TrajOut result) throws Exception {
                return String.format("MN_Q4: device=%s, wkt=%s, bbox=[%.1f,%.1f,%.1f,%.1f], window=[%d-%d]", 
                    result.deviceId, result.wkt, minLon, minLat, maxLon, maxLat, result.winStart, result.winEnd);
            }
        });
        
        formattedResults.addSink(new StringResultCollectorSink("MN_Q4", "Bounded Trajectory"))
                       .name("MN_Q4_Sink");
               
        formattedResults.print("MN_Q4_Output");
    }

    private static void runMN_Q5(StreamExecutionEnvironment env, DataStream<GpsEvent> sourceStream) throws Exception {
        System.out.println("=== Running MN_Q5: Geofenced Trajectory and Speed Analysis ===");
        
        // Define geofence polygon (Brussels city center approximation)
        double[][] geofencePoly = {
            {4.35, 50.84},
            {4.37, 50.84},
            {4.37, 50.86},
            {4.35, 50.86},
            {4.35, 50.84}  // Close the polygon
        };
        double toleranceMeters = 50.0;
        
        DataStream<TrajSpeedWindowFn.TrajSpeedOut> results = MN_Q5.build(env, sourceStream, geofencePoly, toleranceMeters);
        
        // Format and save results
        DataStream<String> formattedResults = results.map(new MapFunction<TrajSpeedWindowFn.TrajSpeedOut, String>() {
            @Override
            public String map(TrajSpeedWindowFn.TrajSpeedOut result) throws Exception {
                return String.format("MN_Q5: device=%s, avgSpeed=%.1f, minSpeed=%.1f, wkt=%s, window=[%d-%d]", 
                    result.deviceId, result.avgSpeed, result.minSpeed, result.wkt, result.winStart, result.winEnd);
            }
        });
        
        formattedResults.addSink(new StringResultCollectorSink("MN_Q5", "Geofenced Trajectory & Speed"))
                       .name("MN_Q5_Sink");
               
        formattedResults.print("MN_Q5_Output");
    }

    private static void runAllQueries(StreamExecutionEnvironment env, DataStream<GpsEvent> sourceStream) throws Exception {
        System.out.println("=== Running ALL Mobility Queries (MN_Q1-Q5) ===");
        
        // Run all queries in parallel
        runMN_Q1(env, sourceStream);
        runMN_Q2(env, sourceStream);
        runMN_Q3(env, sourceStream);
        runMN_Q4(env, sourceStream);
        runMN_Q5(env, sourceStream);
    }

    /**
     * Generate synthetic GPS data for testing mobility queries
     */
    private static List<GpsEvent> generateMobilityTestData() {
        List<GpsEvent> events = new ArrayList<>();
        long baseTime = System.currentTimeMillis();
        
        // Device A: Moving through Brussels center (for MN_Q1, MN_Q5)
        events.add(new GpsEvent("DEVICE_A", 4.352, 50.848, baseTime + 1000, 25.0, 0.2, 0.1));
        events.add(new GpsEvent("DEVICE_A", 4.355, 50.851, baseTime + 2000, 30.0, 0.3, 0.2));
        events.add(new GpsEvent("DEVICE_A", 4.358, 50.854, baseTime + 3000, 35.0, 0.4, 0.3));
        events.add(new GpsEvent("DEVICE_A", 4.361, 50.857, baseTime + 4000, 40.0, 0.5, 0.4));
        events.add(new GpsEvent("DEVICE_A", 4.364, 50.860, baseTime + 5000, 45.0, 0.6, 0.5));
        
        // Device B: Outside exclusion zone with high variance (for MN_Q2)
        events.add(new GpsEvent("DEVICE_B", 4.25, 50.75, baseTime + 1100, 15.0, 0.1, 0.8));
        events.add(new GpsEvent("DEVICE_B", 4.26, 50.76, baseTime + 2100, 20.0, 0.9, 0.2));
        events.add(new GpsEvent("DEVICE_B", 4.27, 50.77, baseTime + 3100, 25.0, 0.3, 0.9));
        events.add(new GpsEvent("DEVICE_B", 4.28, 50.78, baseTime + 4100, 30.0, 0.8, 0.1));
        events.add(new GpsEvent("DEVICE_B", 4.29, 50.79, baseTime + 5100, 35.0, 0.2, 0.7));
        
        // Device C: Trajectory within bounds (for MN_Q3, MN_Q4)
        events.add(new GpsEvent("DEVICE_C", 4.40, 50.10, baseTime + 1200, 50.0, null, null));
        events.add(new GpsEvent("DEVICE_C", 4.41, 50.15, baseTime + 2200, 55.0, null, null));
        events.add(new GpsEvent("DEVICE_C", 4.42, 50.20, baseTime + 3200, 60.0, null, null));
        events.add(new GpsEvent("DEVICE_C", 4.43, 50.25, baseTime + 4200, 65.0, null, null));
        events.add(new GpsEvent("DEVICE_C", 4.44, 50.30, baseTime + 5200, 70.0, null, null));
        
        // Device D: High speed within geofence (for MN_Q5)
        events.add(new GpsEvent("DEVICE_D", 4.360, 50.850, baseTime + 1300, 80.0, null, null));
        events.add(new GpsEvent("DEVICE_D", 4.362, 50.852, baseTime + 2300, 85.0, null, null));
        events.add(new GpsEvent("DEVICE_D", 4.364, 50.854, baseTime + 3300, 90.0, null, null));
        events.add(new GpsEvent("DEVICE_D", 4.366, 50.856, baseTime + 4300, 95.0, null, null));
        
        // Device E: Mixed trajectory spanning multiple areas
        events.add(new GpsEvent("DEVICE_E", 4.30, 50.80, baseTime + 1400, 12.0, 0.1, 0.1));
        events.add(new GpsEvent("DEVICE_E", 4.35, 50.83, baseTime + 2400, 18.0, 0.2, 0.2));
        events.add(new GpsEvent("DEVICE_E", 4.40, 50.86, baseTime + 3400, 22.0, 0.3, 0.3));
        events.add(new GpsEvent("DEVICE_E", 4.45, 50.89, baseTime + 4400, 28.0, 0.4, 0.4));
        
        System.out.println("Generated " + events.size() + " GPS events for testing");
        return events;
    }

    /**
     * Custom sink to collect query results and statistics
     */
    private static class ResultCollectorSink<T> implements SinkFunction<T> {
        private final String queryName;
        private final String queryDescription;
        private long resultCount = 0;
        
        public ResultCollectorSink(String queryName, String queryDescription) {
            this.queryName = queryName;
            this.queryDescription = queryDescription;
        }
        
        @Override
        public void invoke(T value, Context context) throws Exception {
            resultCount++;
            resultCounter.incrementAndGet();
            
            String stats = String.format("%s: %d results processed (%s)", 
                queryName, resultCount, queryDescription);
            executionStats.add(stats);
            
            // Write individual result to file
            writeResultToFile(queryName, value.toString());
        }
    }

    /**
     * Custom sink to collect formatted string results
     */
    private static class StringResultCollectorSink implements SinkFunction<String> {
        private final String queryName;
        private final String queryDescription;
        private long resultCount = 0;
        
        public StringResultCollectorSink(String queryName, String queryDescription) {
            this.queryName = queryName;
            this.queryDescription = queryDescription;
        }
        
        @Override
        public void invoke(String value, Context context) throws Exception {
            resultCount++;
            resultCounter.incrementAndGet();
            
            String stats = String.format("%s: %d results processed (%s)", 
                queryName, resultCount, queryDescription);
            executionStats.add(stats);
            
            // Write individual result to file
            writeResultToFile(queryName, value);
        }
    }

    private static void writeResultToFile(String queryName, String result) {
        try {
            String filename = OUTPUT_DIR + queryName.toLowerCase() + "_results.txt";
            try (FileWriter writer = new FileWriter(filename, true)) {
                writer.write(result + "\n");
            }
        } catch (IOException e) {
            System.err.println("Error writing result to file: " + e.getMessage());
        }
    }

    private static void generateStatisticsReport(String queryRun, long executionTime) {
        try {
            String filename = OUTPUT_DIR + "mobility_query_statistics.txt";
            try (FileWriter writer = new FileWriter(filename)) {
                writer.write("=== Mobility Query Execution Statistics ===\n");
                writer.write("Timestamp: " + Instant.now().toString() + "\n");
                writer.write("Query Run: " + queryRun + "\n");
                writer.write("Total Execution Time: " + executionTime + " ms\n");
                writer.write("Total Results Generated: " + resultCounter.get() + "\n");
                writer.write("\n=== Individual Query Statistics ===\n");
                
                for (String stat : executionStats) {
                    writer.write(stat + "\n");
                }
                
                writer.write("\n=== Query Descriptions ===\n");
                writer.write("MN_Q1: Point-in-Circle Proximity Detection using GeoFlink PointPointRangeQuery\n");
                writer.write("MN_Q2: Variance Analysis with Spatial Polygon Exclusion\n");
                writer.write("MN_Q3: Global Trajectory Computation with GeoFlink Spatial Objects\n");
                writer.write("MN_Q4: Spatially and Temporally Bounded Trajectory Analysis\n");
                writer.write("MN_Q5: Geofenced Trajectory and Speed Monitoring with Polygon Buffering\n");
            }
            System.out.println("Statistics report written to: " + filename);
        } catch (IOException e) {
            System.err.println("Error writing statistics report: " + e.getMessage());
        }
    }
}