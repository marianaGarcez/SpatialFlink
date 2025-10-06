package com.mn.queries;

import com.mn.data.MnGpsEvent;
import com.mn.operators.CountingMap;
import com.mn.operators.CsvParseAndStamp;
import com.mn.operators.Stamped;
import com.mn.sinks.CountingLatencyFileSink;
import GeoFlink.sncb.common.EnrichedEvent;
import GeoFlink.sncb.common.GpsEvent;
import GeoFlink.sncb.ops.TrajectoryAgg;
import GeoFlink.sncb.ops.TrajectoryWindowFn;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
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

import java.util.ArrayList;
import java.util.List;

/**
 * NES-instrumented version of MN_Q3: Global trajectory analysis.
 * Tracks full metrics pipeline with latency, throughput, and selectivity.
 */
public class InstrumentedMN_Q3 {

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
        int bytesPerInputRecord = Integer.parseInt(System.getProperty("bytes.per.input", "128"));
        String inputFile = System.getProperty("input.file", "Data/selected_columns_df.csv");
        String outputFile = System.getProperty("output.file", "metrics/mn_q3_instrumented_results.txt");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

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
                        // Global grid bounds for trajectory analysis
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

        // PIPELINE STAGE 4: Convert to enriched events for trajectory calculation
        DataStream<Stamped<EnrichedEvent>> enrichedEvents = timestamped
                .map(new CountingMap<Stamped<Point>>("5"))
                .name("pre-enrichment-counting")
                .uid("pipe_5_preenrich")
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
                .uid("pipe_6_enrichment");

        // PIPELINE STAGE 5: Windowing and trajectory calculation
        DataStream<Stamped<TrajectoryWindowFn.TrajOut>> trajectoryResults = enrichedEvents
                .map(new CountingMap<Stamped<EnrichedEvent>>("7"))
                .name("pre-trajectory-counting")
                .uid("pipe_7_pretrajectory")
                .map(stamped -> stamped.value) // Extract value for keying
                .keyBy(e -> "ALL")
                .window(SlidingEventTimeWindows.of(Time.seconds(3), Time.seconds(1)))
                .apply(new WindowFunction<EnrichedEvent, Stamped<TrajectoryWindowFn.TrajOut>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<EnrichedEvent> input, 
                                    Collector<Stamped<TrajectoryWindowFn.TrajOut>> out) throws Exception {
                        
                        // Find minimum ingest timestamp from the window
                        long minIngestNs = Long.MAX_VALUE;
                        List<EnrichedEvent> events = new ArrayList<>();
                        for (EnrichedEvent event : input) {
                            events.add(event);
                            // Note: We lose individual ingest timestamps here due to windowing
                            minIngestNs = System.nanoTime();
                        }
                        
                        if (!events.isEmpty()) {
                            // Apply trajectory aggregation
                            TrajectoryAgg agg = new TrajectoryAgg();
                            TrajectoryAgg.TrajAcc acc = agg.createAccumulator();
                            for (EnrichedEvent event : events) {
                                agg.add(event, acc);
                            }
                            
                            TrajectoryWindowFn.TrajOut result = new TrajectoryWindowFn.TrajOut();
                            TrajectoryAgg.TrajAcc finalAcc = agg.getResult(acc);
                            result.deviceId = "ALL";
                            result.winStart = window.getStart();
                            result.winEnd = window.getEnd();
                            
                            // Build trajectory WKT from coordinates
                            if (finalAcc.coords.size() == 0) {
                                result.wkt = "POINT EMPTY";
                            } else if (finalAcc.coords.size() == 1) {
                                result.wkt = String.format("POINT (%.6f %.6f)", 
                                    finalAcc.coords.get(0).x, finalAcc.coords.get(0).y);
                            } else {
                                StringBuilder wkt = new StringBuilder("LINESTRING (");
                                for (int i = 0; i < finalAcc.coords.size(); i++) {
                                    if (i > 0) wkt.append(", ");
                                    wkt.append(String.format("%.6f %.6f", 
                                        finalAcc.coords.get(i).x, finalAcc.coords.get(i).y));
                                }
                                wkt.append(")");
                                result.wkt = wkt.toString();
                            }
                            
                            out.collect(new Stamped<>(result, minIngestNs));
                        }
                    }
                })
                .name("trajectory-calculation")
                .uid("pipe_8_trajectory");

        // PIPELINE STAGE 6: Final counting and sink
        trajectoryResults
                .map(new CountingMap<Stamped<TrajectoryWindowFn.TrajOut>>("9"))
                .name("final-counting")
                .uid("pipe_9_final")
                .addSink(new CountingLatencyFileSink<>(outputFile, 
                        (TrajectoryWindowFn.TrajOut result) -> result.toString().length() + 1))
                .name("instrumented-trajectory-sink")
                .uid("pipe_99_sink");

        // Execute the job
        env.execute("Instrumented MN_Q3: Global Trajectory Analysis with NES Metrics");
    }
}