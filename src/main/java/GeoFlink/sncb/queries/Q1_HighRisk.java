package GeoFlink.sncb.queries;

import GeoFlink.sncb.common.*;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.spatialOperators.range.PointPolygonRangeQuery;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.MapFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.prep.PreparedGeometry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Q1_HighRisk {

    public static DataStream<EnrichedEvent> build(StreamExecutionEnvironment env, DataStream<GpsEvent> events,
                                                  List<PreparedGeometry> highRiskBuffered) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Define grid bounds
        double minX = -180.0, maxX = 180.0, minY = -90.0, maxY = 90.0;
        UniformGrid uGrid = new UniformGrid(100, minX, maxX, minY, maxY);

        // Convert GPS events to GeoFlink Points
        DataStream<Point> pointStream = events.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<GpsEvent>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(GpsEvent e) { return e.ts; }
                })
                .map(new MapFunction<GpsEvent, Point>() {
                    @Override
                    public Point map(GpsEvent gps) throws Exception {
                        return new Point(gps.deviceId, gps.lon, gps.lat, gps.ts, uGrid);
                    }
                });

        // Convert PreparedGeometry to GeoFlink Polygons
        Set<Polygon> highRiskPolygons = new HashSet<>();
        for (PreparedGeometry pg : highRiskBuffered) {
            org.locationtech.jts.geom.Geometry geom = pg.getGeometry();
            if (geom instanceof org.locationtech.jts.geom.Polygon) {
                org.locationtech.jts.geom.Polygon jtsPolygon = (org.locationtech.jts.geom.Polygon) geom;
                
                // Convert JTS polygon to GeoFlink polygon format
                List<List<Coordinate>> coordinates = new ArrayList<>();
                List<Coordinate> outerRing = new ArrayList<>();
                Coordinate[] coords = jtsPolygon.getExteriorRing().getCoordinates();
                for (Coordinate coord : coords) {
                    outerRing.add(coord);
                }
                coordinates.add(outerRing);
                
                Polygon geoPolygon = new Polygon(coordinates, uGrid);
                highRiskPolygons.add(geoPolygon);
            }
        }

        // Configure GeoFlink range query for proximity detection
        QueryConfiguration qConfig = new QueryConfiguration(QueryType.RealTime);
        PointPolygonRangeQuery rangeQuery = new PointPolygonRangeQuery(qConfig, uGrid);
        
        // Execute range query to find points near high-risk areas
        // Note: Using a small radius for proximity detection
        DataStream<Point> nearRisk = rangeQuery.run(pointStream, highRiskPolygons, 0.001); // Small radius for close proximity

        // Convert back to EnrichedEvent format for consistency
        DataStream<EnrichedEvent> enrichedNearRisk = nearRisk.map(new MapFunction<Point, EnrichedEvent>() {
            @Override
            public EnrichedEvent map(Point p) throws Exception {
                GpsEvent gps = new GpsEvent();
                gps.deviceId = p.objID;
                gps.ts = p.timeStampMillisec;
                gps.lon = p.point.getX();
                gps.lat = p.point.getY();
                
                EnrichedEvent enriched = new EnrichedEvent();
                enriched.raw = gps;
                enriched.ptWgs84 = p.point;
                return enriched;
            }
        });

        return enrichedNearRisk
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<EnrichedEvent, EnrichedEvent, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<EnrichedEvent> elements, Collector<EnrichedEvent> out) {
                        for (EnrichedEvent e : elements) out.collect(e);
                    }
                });
    }
}
