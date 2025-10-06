package GeoFlink.sncb.tests;

import GeoFlink.sncb.common.GpsEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SyntheticGpsSource implements SourceFunction<GpsEvent> {
    private final double minLon, maxLon, minLat, maxLat;
    private final long targetEps;
    private final long durationMs;
    private final int numDevices;
    private volatile boolean running = true;

    public SyntheticGpsSource(double minLon, double maxLon, double minLat, double maxLat,
                              long targetEps, long durationMs, int numDevices) {
        this.minLon = minLon; this.maxLon = maxLon; this.minLat = minLat; this.maxLat = maxLat;
        this.targetEps = targetEps; this.durationMs = durationMs; this.numDevices = numDevices;
    }

    @Override
    public void run(SourceContext<GpsEvent> ctx) throws Exception {
        Random rnd = new Random(42);
        long start = System.currentTimeMillis();
        long emittedThisSecond = 0;
        long currentSecondStart = start;
        long sleepChunk = 1; // ms

        while (running && System.currentTimeMillis() - start < durationMs) {
            long now = System.currentTimeMillis();
            if (now - currentSecondStart >= 1000) {
                currentSecondStart = now;
                emittedThisSecond = 0;
            }
            if (emittedThisSecond < targetEps) {
                long toEmit = Math.min(1000, targetEps - emittedThisSecond); // batch
                for (int i = 0; i < toEmit; i++) {
                    int dev = rnd.nextInt(numDevices);
                    String deviceId = "D" + dev;
                    double lon = minLon + rnd.nextDouble() * (maxLon - minLon);
                    double lat = minLat + rnd.nextDouble() * (maxLat - minLat);
                    long ts = System.currentTimeMillis();
                    double speed = 10 + rnd.nextDouble() * 60; // m/s
                    double FA = rnd.nextDouble();
                    double FF = rnd.nextDouble();
                    ctx.collect(new GpsEvent(deviceId, lon, lat, ts, speed, FA, FF));
                }
                emittedThisSecond += toEmit;
            } else {
                Thread.sleep(sleepChunk);
            }
        }
    }

    @Override
    public void cancel() { running = false; }
}

