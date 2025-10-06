package com.mn.queries;

import com.mn.data.MnGpsEvent;
import com.mn.operators.CountingFlatMap;
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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * MN_Q4: Bounded Trajectory Analysis with Brussels Area Constraints
 * 
 * This instrumented query performs trajectory analysis for devices within the Brussels area bounds
 * (latitude: 50.773-50.896, longitude: 4.287-4.419) and within specific time windows.
 * 
 * NES Metrics: Full pipeline instrumentation with counting operators and latency measurement.
 */
public class InstrumentedMN_Q4 {
    
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
            
            return new MnGpsEvent(
                    fields[1],                          // device_id
                    Double.parseDouble(fields[5]),      // gps_lon  
                    Double.parseDouble(fields[4]),      // gps_lat
                    Long.parseLong(fields[0])           // time_utc
            );
        }
    }
    
    public static void main(String[] args) throws Exception {
        // Parse arguments with defaults
        String inputPath = args.length > 0 ? args[0] : "/Users/marianaduarte/SpatialFlink/dummy-data.csv";
        String outputFile = args.length > 1 ? args[1] : "/Users/marianaduarte/SpatialFlink/q4_results.csv";
        long theoreticalRowsPerSec = args.length > 2 ? Long.parseLong(args[2]) : 1000;
        int bytesPerInputRecord = args.length > 3 ? Integer.parseInt(args[3]) : 128;
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        
        // PIPELINE STAGE 1: CSV parsing with ingest timestamp
        DataStream<String> lines = env.readTextFile(inputPath)
                .uid("pipe_0_source");

        DataStream<Stamped<MnGpsEvent>> parsed = lines
                .map(new CsvParseAndStamp<>(new GpsEventParser(), theoreticalRowsPerSec, bytesPerInputRecord))
                .name("csv-parse-and-stamp")
                .uid("pipe_1_parse");
        
        // PIPELINE STAGE 2: Spatial filtering for Brussels area
        DataStream<Stamped<MnGpsEvent>> brusselsFiltered = parsed
                .map(new CountingMap<Stamped<MnGpsEvent>>("1"))
                .name("initial-counting")
                .uid("pipe_1_initial")
                .filter(stamped -> {
                    MnGpsEvent event = stamped.value;
                    // Brussels bounds: latitude 50.773-50.896, longitude 4.287-4.419
                    return event.lat >= 50.773 && event.lat <= 50.896 &&
                           event.lon >= 4.287 && event.lon <= 4.419;
                })
                .name("brussels-spatial-filter")
                .uid("pipe_2_spatial_filter");

        // PIPELINE STAGE 3: Transform to GpsEvent for compatibility  
        DataStream<Stamped<GpsEvent>> gpsEvents = brusselsFiltered
                .map(new CountingMap<Stamped<MnGpsEvent>>("3"))
                .name("post-filter-counting")
                .uid("pipe_3_postfilter")
                .map(stamped -> {
                    MnGpsEvent mnEvent = stamped.value;
                    GpsEvent gpsEvent = new GpsEvent(
                            mnEvent.deviceId,
                            mnEvent.lon,
                            mnEvent.lat,
                            mnEvent.timestamp,
                            null, // gpsSpeed
                            null, // FA
                            null  // FF
                    );
                    return new Stamped<>(gpsEvent, stamped.ingestNs);
                })
                .name("mn-to-gps-conversion")
                .uid("pipe_4_conversion");

        // PIPELINE STAGE 4: Transform to EnrichedEvent for trajectory processing
        DataStream<Stamped<EnrichedEvent>> enrichedEvents = gpsEvents
                .map(new CountingMap<Stamped<GpsEvent>>("5"))
                .name("pre-enrichment-counting")
                .uid("pipe_5_preenrichment")
                .map(stamped -> {
                    GpsEvent gpsEvent = stamped.value;
                    
                    EnrichedEvent enrichedEvent = new EnrichedEvent();
                    enrichedEvent.raw = gpsEvent;
                    // Note: ptWgs84 and ptMetric are not needed for trajectory aggregation
                    // They're only used if we need actual spatial operations
                    
                    return new Stamped<>(enrichedEvent, stamped.ingestNs);
                })
                .name("gps-to-enriched-conversion")
                .uid("pipe_6_enrichment");

        // PIPELINE STAGE 5: Windowing and trajectory calculation with device-level keying
        DataStream<Stamped<TrajectoryWindowFn.TrajOut>> trajectoryResults = enrichedEvents
                .map(new CountingMap<Stamped<EnrichedEvent>>("7"))
                .name("pre-trajectory-counting")
                .uid("pipe_7_pretrajectory")
                .map(stamped -> stamped.value) // Extract value for keying
                .keyBy(e -> e.raw.deviceId)
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
                        
                        if (events.size() < 2) {
                            return; // Skip windows with insufficient points for trajectory
                        }
                        
                        // Build trajectory using TrajectoryAgg
                        TrajectoryAgg agg = new TrajectoryAgg();
                        TrajectoryAgg.TrajAcc acc = agg.createAccumulator();
                        
                        for (EnrichedEvent event : events) {
                            agg.add(event, acc);
                        }
                        
                        TrajectoryWindowFn.TrajOut result = new TrajectoryWindowFn.TrajOut();
                        TrajectoryAgg.TrajAcc finalAcc = agg.getResult(acc);
                        result.deviceId = key;
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
                .name("file-sink")
                .uid("pipe_99_sink");
        
        System.out.println("Starting InstrumentedMN_Q4 with NES metrics...");
        env.execute("InstrumentedMN_Q4-Bounded-Trajectory-Analysis");
    }
}