package com.mn.queries;

import com.mn.data.MnGpsEvent;
import com.mn.operators.CountingFlatMap;
import com.mn.operators.CountingMap;
import com.mn.operators.CsvParseAndStamp;
import com.mn.operators.Stamped;
import com.mn.sinks.CountingLatencyFileSink;
import GeoFlink.sncb.common.EnrichedEvent;
import GeoFlink.sncb.common.GpsEvent;
import GeoFlink.sncb.ops.VarianceAgg;
import GeoFlink.sncb.ops.VarianceWindowFn;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;

import java.util.ArrayList;
import java.util.List;

/**
 * NES-instrumented version of MN_Q2: Variance analysis with spatial exclusion.
 * Tracks full metrics pipeline with latency, throughput, and selectivity.
 */
public class InstrumentedMN_Q2 {

    /**
     * CSV parser for GPS events from real data file.
     */
    public static class GpsEventParser implements CsvParseAndStamp.Parser<MnGpsEvent>, java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        @Override
        public MnGpsEvent parse(String line) throws Exception {
            String[] fields = line.trim().split(",");
            if (fields.length < 14) {
                throw new IllegalArgumentException("Invalid CSV line: " + line);
            }
            
            long timestamp = Long.parseLong(fields[0].trim()) * 1000L; // Convert to milliseconds
            String deviceId = fields[1].trim();
            double bp = Double.parseDouble(fields[3].trim()); // PCFA_mbar
            double ff = Double.parseDouble(fields[4].trim()); // PCFF_mbar
            double lat = Double.parseDouble(fields[12].trim()); // gps_lat
            double lon = Double.parseDouble(fields[13].trim()); // gps_lon
            
            return new MnGpsEvent(deviceId, lon, lat, timestamp, bp, ff);
        }
    }

    /**
     * Serializable function to estimate VarOut result size for NES byte tracking.
     */
    public static class VarOutSizeEstimator implements java.util.function.ToIntFunction<VarianceWindowFn.VarOut>, java.io.Serializable {
        private static final long serialVersionUID = 1L;
        
        @Override
        public int applyAsInt(VarianceWindowFn.VarOut result) {
            return result.toString().length() + 1; // +1 for newline
        }
    }

    public static void main(String[] args) throws Exception {
        // Configuration parameters - 20K EPS via TCP streaming
        long theoreticalRowsPerSec = Long.parseLong(System.getProperty("rows.per.sec", "20000"));
        int bytesPerInputRecord = Integer.parseInt(System.getProperty("bytes.per.input", "128"));
        String tcpHost = System.getProperty("tcp.host", "localhost");
        int tcpPort = Integer.parseInt(System.getProperty("tcp.port", "32323"));
        String outputFile = System.getProperty("output.file", "metrics/mn_q2_instrumented_results.txt");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // PIPELINE STAGE 1: TCP streaming source with 20K EPS target
        DataStream<String> lines = env.socketTextStream(tcpHost, tcpPort)
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

        // PIPELINE STAGE 4: Spatial filtering (polygon exclusion) with counting
        DataStream<Stamped<Point>> filteredPoints = timestamped
                .map(new CountingMap<Stamped<Point>>("5"))
                .name("pre-filter-counting")
                .uid("pipe_5_prefilter")
                .flatMap(new CountingFlatMap<Stamped<Point>, Stamped<Point>>("6", 
                        new CountingFlatMap.FlatMapFunction<Stamped<Point>, Stamped<Point>>() {
                    private transient Polygon excludePolygon;
                    
                    @Override
                    public void flatMap(Stamped<Point> stamped, Collector<Stamped<Point>> out) throws Exception {
                        if (excludePolygon == null) {
                            // Create polygon for exclusion area (4.0..4.6 x 50.0..50.8)
                            double minX = 4.0, maxX = 5.0, minY = 50.0, maxY = 51.0;
                            UniformGrid uGrid = new UniformGrid(100, minX, maxX, minY, maxY);
                            
                            List<List<Coordinate>> coordinates = new ArrayList<>();
                            List<Coordinate> outerRing = new ArrayList<>();
                            outerRing.add(new Coordinate(4.0, 50.0));
                            outerRing.add(new Coordinate(4.0, 50.8));
                            outerRing.add(new Coordinate(4.6, 50.8));
                            outerRing.add(new Coordinate(4.6, 50.0));
                            outerRing.add(new Coordinate(4.0, 50.0));
                            coordinates.add(outerRing);
                            
                            excludePolygon = new Polygon(coordinates, uGrid);
                        }
                        
                        Point point = stamped.value;
                        // Filter points that are NOT in the polygon
                        if (!excludePolygon.polygon.intersects(point.point)) {
                            out.collect(stamped);
                        }
                        // Points inside polygon are filtered out (selectivity measurement)
                    }
                }))
                .name("spatial-exclusion-filter")
                .uid("pipe_6_spatial_filter");

        // PIPELINE STAGE 5: Convert to enriched events for variance calculation
        DataStream<Stamped<EnrichedEvent>> enrichedEvents = filteredPoints
                .map(new CountingMap<Stamped<Point>>("7"))
                .name("pre-enrichment-counting")
                .uid("pipe_7_preenrich")
                .map(new RichMapFunction<Stamped<Point>, Stamped<EnrichedEvent>>() {
                    private Counter enrichmentConversions;
                    
                    @Override
                    public void open(Configuration parameters) {
                        enrichmentConversions = getRuntimeContext().getMetricGroup().counter("enrichment_conversions");
                    }

                    @Override
                    public Stamped<EnrichedEvent> map(Stamped<Point> stamped) throws Exception {
                        Point p = stamped.value;
                        
                        GpsEvent gps = new GpsEvent();
                        gps.deviceId = p.objID;
                        gps.ts = p.timeStampMillisec;
                        gps.lon = p.point.getX();
                        gps.lat = p.point.getY();
                        
                        EnrichedEvent enriched = new EnrichedEvent();
                        enriched.raw = gps;
                        enriched.ptWgs84 = p.point;
                        
                        enrichmentConversions.inc();
                        return new Stamped<>(enriched, stamped.ingestNs);
                    }
                })
                .name("enriched-event-conversion")
                .uid("pipe_8_enrichment");

        // PIPELINE STAGE 6: Windowing and variance calculation
        DataStream<Stamped<VarianceWindowFn.VarOut>> varianceResults = enrichedEvents
                .map(new CountingMap<Stamped<EnrichedEvent>>("9"))
                .name("pre-variance-counting")
                .uid("pipe_9_prevariance")
                .map(stamped -> stamped.value) // Extract value for keying
                .keyBy(e -> "ALL")
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.milliseconds(200)))
                .apply(new WindowFunction<EnrichedEvent, Stamped<VarianceWindowFn.VarOut>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<EnrichedEvent> input, 
                                    Collector<Stamped<VarianceWindowFn.VarOut>> out) throws Exception {
                        
                        // Find minimum ingest timestamp from the window
                        long minIngestNs = Long.MAX_VALUE;
                        List<EnrichedEvent> events = new ArrayList<>();
                        for (EnrichedEvent event : input) {
                            events.add(event);
                            // Note: We lose individual ingest timestamps here due to windowing
                            // Using current time as approximation
                            minIngestNs = System.nanoTime();
                        }
                        
                        if (!events.isEmpty()) {
                            // Apply variance aggregation
                            VarianceAgg agg = new VarianceAgg();
                            VarianceAgg.VarAcc acc = agg.createAccumulator();
                            for (EnrichedEvent event : events) {
                                agg.add(event, acc);
                            }
                            
                            VarianceAgg.VarAcc finalAcc = agg.getResult(acc);
                            VarianceWindowFn.VarOut result = new VarianceWindowFn.VarOut();
                            result.varBP = VarianceAgg.variance(finalAcc.n, finalAcc.sumFA, finalAcc.sumSqFA);
                            result.varFF = VarianceAgg.variance(finalAcc.n, finalAcc.sumFF, finalAcc.sumSqFF);
                            result.start = window.getStart();
                            result.end = window.getEnd();
                            result.cnt = finalAcc.n;
                            
                            out.collect(new Stamped<>(result, minIngestNs));
                        }
                    }
                })
                .name("variance-calculation")
                .uid("pipe_10_variance");

        // PIPELINE STAGE 7: Final counting and sink
        varianceResults
                .map(new CountingMap<Stamped<VarianceWindowFn.VarOut>>("11"))
                .name("final-counting")
                .uid("pipe_11_final")
                .addSink(new CountingLatencyFileSink<>(outputFile, new VarOutSizeEstimator()))
                .name("instrumented-variance-sink")
                .uid("pipe_99_sink");

        // Execute the job
        env.execute("Instrumented MN_Q2: Variance Analysis with NES Metrics");
    }
}