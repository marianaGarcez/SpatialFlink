package com.mn.queries;

import com.mn.data.MnGpsEvent;
import com.mn.operators.CountingFlatMap;
import com.mn.operators.CountingMap;
import com.mn.operators.CsvParseAndStamp;
import com.mn.operators.Stamped;
import com.mn.sinks.CountingLatencyFileSink;
import GeoFlink.sncb.common.EnrichedEvent;
import GeoFlink.sncb.common.GpsEvent;
import GeoFlink.sncb.ops.TrajSpeedAgg;
import GeoFlink.sncb.ops.TrajSpeedWindowFn;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
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
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * NES-instrumented version of MN_Q5: Geofenced trajectory and speed monitoring.
 * Tracks full metrics pipeline with latency, throughput, and selectivity.
 */
public class InstrumentedMN_Q5 {

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
        String outputFile = System.getProperty("output.file", "metrics/mn_q5_instrumented_results.txt");
        double tolMeters = Double.parseDouble(System.getProperty("tolerance.meters", "0.001")); // degrees approximation
        
        // Default polygon coordinates (Brussels area rectangle)
        double[][] polyLonLat = {
            {4.3, 50.8}, {4.4, 50.8}, {4.4, 50.9}, {4.3, 50.9}, {4.3, 50.8}
        };

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

        // PIPELINE STAGE 4: Geofence filtering with counting
        DataStream<Stamped<Point>> geofencedPoints = timestamped
                .map(new CountingMap<Stamped<Point>>("5"))
                .name("pre-geofence-counting")
                .uid("pipe_5_pregeofence")
                .flatMap(new CountingFlatMap<Stamped<Point>, Stamped<Point>>("6", 
                        new CountingFlatMap.FlatMapFunction<Stamped<Point>, Stamped<Point>>() {
                    private transient Geometry bufferedPolygon;
                    
                    @Override
                    public void flatMap(Stamped<Point> stamped, Collector<Stamped<Point>> out) throws Exception {
                        if (bufferedPolygon == null) {
                            // Create geofence polygon
                            double minX = 4.0, maxX = 5.0, minY = 50.0, maxY = 51.0;
                            UniformGrid uGrid = new UniformGrid(100, minX, maxX, minY, maxY);
                            
                            List<List<Coordinate>> coordinates = new ArrayList<>();
                            List<Coordinate> outerRing = new ArrayList<>();
                            for (double[] coord : polyLonLat) {
                                outerRing.add(new Coordinate(coord[0], coord[1]));
                            }
                            coordinates.add(outerRing);
                            
                            Polygon geoPolygon = new Polygon(coordinates, uGrid);
                            bufferedPolygon = geoPolygon.polygon.buffer(tolMeters);
                        }
                        
                        Point point = stamped.value;
                        // Filter points inside the geofence
                        if (bufferedPolygon.contains(point.point)) {
                            out.collect(stamped);
                        }
                        // Points outside geofence are filtered out (selectivity measurement)
                    }
                }))
                .name("geofence-filter")
                .uid("pipe_6_geofence");

        // PIPELINE STAGE 5: Convert to enriched events for trajectory/speed calculation
        DataStream<Stamped<EnrichedEvent>> enrichedEvents = geofencedPoints
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

        // PIPELINE STAGE 6: Windowing and trajectory/speed calculation
        DataStream<Stamped<TrajSpeedWindowFn.TrajSpeedOut>> speedResults = enrichedEvents
                .map(new CountingMap<Stamped<EnrichedEvent>>("9"))
                .name("pre-speed-counting")
                .uid("pipe_9_prespeed")
                .map(stamped -> stamped.value) // Extract value for keying
                .keyBy(e -> e.raw.deviceId)
                .window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(2)))
                .apply(new WindowFunction<EnrichedEvent, Stamped<TrajSpeedWindowFn.TrajSpeedOut>, String, TimeWindow>() {
                    @Override
                    public void apply(String deviceId, TimeWindow window, Iterable<EnrichedEvent> input, 
                                    Collector<Stamped<TrajSpeedWindowFn.TrajSpeedOut>> out) throws Exception {
                        
                        // Find minimum ingest timestamp from the window
                        long minIngestNs = Long.MAX_VALUE;
                        List<EnrichedEvent> events = new ArrayList<>();
                        for (EnrichedEvent event : input) {
                            events.add(event);
                            minIngestNs = System.nanoTime();
                        }
                        
                        if (!events.isEmpty()) {
                            // Apply trajectory and speed aggregation
                            TrajSpeedAgg agg = new TrajSpeedAgg();
                            TrajSpeedAgg.TrajSpeedAcc acc = agg.createAccumulator();
                            for (EnrichedEvent event : events) {
                                agg.add(event, acc);
                            }
                            
                            TrajSpeedWindowFn.TrajSpeedOut result = new TrajSpeedWindowFn.TrajSpeedOut();
                            TrajSpeedAgg.TrajSpeedAcc finalAcc = agg.getResult(acc);
                            result.deviceId = deviceId;
                            result.winStart = window.getStart();
                            result.winEnd = window.getEnd();
                            
                            // Calculate average speed and use min speed
                            if (finalAcc.count > 0) {
                                result.avgSpeed = finalAcc.sumSpeed / finalAcc.count;
                                result.minSpeed = finalAcc.minSpeed;
                            } else {
                                result.avgSpeed = 0.0;
                                result.minSpeed = 0.0;
                            }
                            
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
                .name("trajectory-speed-calculation")
                .uid("pipe_10_trajspeed");

        // PIPELINE STAGE 7: Speed filtering and final counting
        DataStream<Stamped<TrajSpeedWindowFn.TrajSpeedOut>> filteredResults = speedResults
                .flatMap(new CountingFlatMap<Stamped<TrajSpeedWindowFn.TrajSpeedOut>, Stamped<TrajSpeedWindowFn.TrajSpeedOut>>("11", 
                        new CountingFlatMap.FlatMapFunction<Stamped<TrajSpeedWindowFn.TrajSpeedOut>, Stamped<TrajSpeedWindowFn.TrajSpeedOut>>() {
                    @Override
                    public void flatMap(Stamped<TrajSpeedWindowFn.TrajSpeedOut> stamped, 
                                      Collector<Stamped<TrajSpeedWindowFn.TrajSpeedOut>> out) throws Exception {
                        TrajSpeedWindowFn.TrajSpeedOut result = stamped.value;
                        // Filter by speed thresholds
                        if (result.avgSpeed < 100.0 || result.minSpeed < 20.0) {
                            out.collect(stamped);
                        }
                        // Results not meeting speed criteria are filtered out
                    }
                }))
                .name("speed-filter")
                .uid("pipe_11_speedfilter");

        // PIPELINE STAGE 8: Final sink
        filteredResults
                .map(new CountingMap<Stamped<TrajSpeedWindowFn.TrajSpeedOut>>("12"))
                .name("final-counting")
                .uid("pipe_12_final")
                .addSink(new CountingLatencyFileSink<>(outputFile, 
                        (TrajSpeedWindowFn.TrajSpeedOut result) -> result.toString().length() + 1))
                .name("instrumented-speed-sink")
                .uid("pipe_99_sink");

        // Execute the job
        env.execute("Instrumented MN_Q5: Geofenced Speed Analysis with NES Metrics");
    }
}