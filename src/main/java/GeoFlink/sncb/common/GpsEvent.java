package GeoFlink.sncb.common;

public class GpsEvent {
    public String deviceId;
    public double lon;
    public double lat;
    public long ts;
    public Double gpsSpeed; // m/s
    public Double FA;       // brake pressure A (bar)
    public Double FF;       // brake pressure F (bar)

    public GpsEvent() {}

    public GpsEvent(String deviceId, double lon, double lat, long ts, Double gpsSpeed, Double FA, Double FF) {
        this.deviceId = deviceId;
        this.lon = lon;
        this.lat = lat;
        this.ts = ts;
        this.gpsSpeed = gpsSpeed;
        this.FA = FA;
        this.FF = FF;
    }
}

