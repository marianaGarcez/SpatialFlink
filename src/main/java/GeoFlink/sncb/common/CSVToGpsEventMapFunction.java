package GeoFlink.sncb.common;

import org.apache.flink.api.common.functions.MapFunction;

public class CSVToGpsEventMapFunction implements MapFunction<String, GpsEvent> {
    private final String delimiter;

    public CSVToGpsEventMapFunction(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public GpsEvent map(String line) {
        String[] f = line.split(delimiter, -1);
        // Expect schema order as provided
        long ts = parseLong(f, 0);
        String deviceId = f[1];
        Double pcfa = parseDouble(f, 3);
        Double pcff = parseDouble(f, 4);
        Double speed = parseDouble(f, 11);
        double lat = parseDouble(f, 12);
        double lon = parseDouble(f, 13);
        return new GpsEvent(deviceId, lon, lat, ts, speed, pcfa, pcff);
    }

    private static long parseLong(String[] f, int i) {
        try { return Long.parseLong(f[i].trim()); } catch (Exception e) { return 0L; }
    }
    private static double parseDouble(String[] f, int i) {
        try { return Double.parseDouble(f[i].trim()); } catch (Exception e) { return 0.0; }
    }
}

