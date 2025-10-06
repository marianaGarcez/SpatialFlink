package com.mn.queries;

import com.mn.data.MnGpsEvent;
import com.mn.metrics.MetricNames;
import com.mn.operators.CountingMap;
import com.mn.operators.CsvParseAndStamp;
import com.mn.operators.Stamped;
import com.mn.sinks.CountingLatencyFileSink;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.spatialOperators.range.PointPointRangeQuery;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * NES-instrumented version of MN_Q1: Point-in-circle proximity detection.
 * Tracks full metrics pipeline with latency, throughput, and selectivity.
 */
public class InstrumentedMN_Q1 {
    
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
        
        @Override
        public String toString() {
            return String.format("MN_Q1: count=%d, window=[%d-%d]", cnt, start, end);
        }
    }

    /**
     * CSV parser for GPS events from real data file.
     */
    public static class GpsEventParser implements CsvParseAndStamp.Parser<MnGpsEvent> {
        @Override
        public MnGpsEvent parse(String line) throws Exception {
            String[] fields = line.trim().split(",");
            if (fields.length < 14) {
                throw new IllegalArgumentException("Invalid CSV line: " + line);
            }
            
            long timestamp = Long.parseLong(fields[0].trim()) * 1000L; // Convert to milliseconds
            String deviceId = fields[1].trim();
            double lat = Double.parseDouble(fields[12].trim()); // gps_lat
            double lon = Double.parseDouble(fields[13].trim()); // gps_lon
            
            return new MnGpsEvent(deviceId, lon, lat, timestamp);
        }
    }

    public static void main(String[] args) throws Exception {
        // Configuration parameters
        long theoreticalRowsPerSec = Long.parseLong(System.getProperty("rows.per.sec", "20000"));
        int bytesPerInputRecord = Integer.parseInt(System.getProperty("bytes.per.input", "128")); // CSV line size estimate
        double queryLon = Double.parseDouble(System.getProperty("query.lon", "4.3658"));
        double queryLat = Double.parseDouble(System.getProperty("query.lat", "50.6456"));
        double tolMeters = Double.parseDouble(System.getProperty("tolerance.meters", "100.0"));
        String inputFile = System.getProperty("input.file", "Data/selected_columns_df.csv");
        String outputFile = System.getProperty("output.file", "metrics/mn_q1_instrumented_results.txt");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1); // Single parallelism for easier metrics aggregation

        // PIPELINE STAGE 1: CSV parsing with ingest timestamp
        DataStream<String> lines = env.readTextFile(inputFile)
                .uid("pipe_0_source");

        DataStream<Stamped<MnGpsEvent>> parsed = lines
                .map(new CsvParseAndStamp<>(new GpsEventParser(), theoreticalRowsPerSec, bytesPerInputRecord))
                .name("csv-parse-and-stamp")
                .uid("pipe_1_parse");

        // PIPELINE STAGE 2: Convert to GeoFlink Points with counting
        DataStream<Stamped<Point>> points = parsed
                .map(new CountingMap<Stamped<MnGpsEvent>>("2"))
                .name("counting-wrapper")
                .uid("pipe_2_count")
                .map(new RichMapFunction<Stamped<MnGpsEvent>, Stamped<Point>>() {
                    private transient UniformGrid uGrid;
                    private Counter pointConversions;
                    
                    @Override
                    public void open(Configuration parameters) {
                        // Define grid bounds for Brussels area
                        double minX = 4.0, maxX = 5.0, minY = 50.0, maxY = 51.0;
                        uGrid = new UniformGrid(100, minX, maxX, minY, maxY);
                        pointConversions = getRuntimeContext().getMetricGroup().counter("point_conversions");
                    }

                    @Override
                    public Stamped<Point> map(Stamped<MnGpsEvent> stamped) throws Exception {
                        MnGpsEvent gps = stamped.value;
                        Point point = new Point(gps.deviceId, gps.lon, gps.lat, gps.timestamp, uGrid);
                        pointConversions.inc();
                        return new Stamped<>(point, stamped.ingestNs);
                    }
                })
                .name("geoflink-point-conversion")
                .uid("pipe_3_geopoint");

        // PIPELINE STAGE 3: Assign timestamps and watermarks
        DataStream<Stamped<Point>> timestamped = points
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<Stamped<Point>>(Time.seconds(2)) {
                            @Override 
                            public long extractTimestamp(Stamped<Point> stamped) { 
                                return stamped.value.timeStampMillisec; 
                            }
                        })
                .name("timestamp-assignment")
                .uid("pipe_4_timestamp");

        // PIPELINE STAGE 4: GeoFlink range query with counting
        DataStream<Stamped<Point>> withinRange = timestamped
                .map(new CountingMap<Stamped<Point>>("5"))
                .name("pre-range-counting")
                .uid("pipe_5_prerange")
                .map(new RichMapFunction<Stamped<Point>, Stamped<Point>>() {
                    private transient PointPointRangeQuery rangeQuery;
                    private transient Point queryPoint;
                    private transient HashSet<Point> queryPoints;
                    private Counter rangeQueries;

                    @Override
                    public void open(Configuration parameters) {
                        double minX = 4.0, maxX = 5.0, minY = 50.0, maxY = 51.0;
                        UniformGrid uGrid = new UniformGrid(100, minX, maxX, minY, maxY);
                        QueryConfiguration qConfig = new QueryConfiguration(QueryType.RealTime);
                        rangeQuery = new PointPointRangeQuery(qConfig, uGrid);
                        
                        queryPoint = new Point("query", queryLon, queryLat, 0L, uGrid);
                        queryPoints = new HashSet<>();
                        queryPoints.add(queryPoint);
                        
                        rangeQueries = getRuntimeContext().getMetricGroup().counter("range_queries");
                    }

                    @Override
                    public Stamped<Point> map(Stamped<Point> stamped) throws Exception {
                        rangeQueries.inc();
                        // Simple distance check for demonstration
                        Point p = stamped.value;
                        double distance = Math.sqrt(Math.pow(p.point.getX() - queryLon, 2) + 
                                                  Math.pow(p.point.getY() - queryLat, 2)) * 111320; // rough meters
                        
                        if (distance <= tolMeters) {
                            return stamped; // Point is within range
                        } else {
                            return null; // Filter out
                        }
                    }
                })
                .filter(stamped -> stamped != null)
                .name("geoflink-range-query")
                .uid("pipe_6_range");

        // PIPELINE STAGE 5: Windowing and counting
        DataStream<Stamped<CountOut>> results = withinRange
                .map(new CountingMap<Stamped<Point>>("7"))
                .name("pre-window-counting")
                .uid("pipe_7_prewindow")
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessAllWindowFunction<Stamped<Point>, Stamped<CountOut>, TimeWindow>() {
                    @Override
                    public void process(Context ctx, Iterable<Stamped<Point>> elements, Collector<Stamped<CountOut>> out) {
                        long count = 0;
                        long minIngestNs = Long.MAX_VALUE;
                        
                        for (Stamped<Point> element : elements) {
                            count++;
                            minIngestNs = Math.min(minIngestNs, element.ingestNs);
                        }
                        
                        CountOut result = new CountOut(ctx.window().getStart(), ctx.window().getEnd(), count);
                        Stamped<CountOut> stamped = new Stamped<>(result, 
                                count > 0 ? minIngestNs : System.nanoTime());
                        out.collect(stamped);
                    }
                })
                .name("windowed-counting")
                .uid("pipe_8_window");

        // PIPELINE STAGE 6: Final counting and sink
        results
                .map(new CountingMap<Stamped<CountOut>>("9"))
                .name("final-counting")
                .uid("pipe_9_final")
                .addSink(new CountingLatencyFileSink<>(outputFile, 
                        (CountOut result) -> result.toString().length() + 1)) // +1 for newline
                .name("instrumented-file-sink")
                .uid("pipe_99_sink");

        // Execute the job
        env.execute("Instrumented MN_Q1: Point Proximity with NES Metrics");
    }
}