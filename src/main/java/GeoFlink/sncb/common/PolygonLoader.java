package GeoFlink.sncb.common;

import org.json.JSONArray;
import org.json.JSONObject;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.io.WKTReader;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class PolygonLoader {
    private final GeometryFactory gf25831 = new GeometryFactory(new PrecisionModel(), 25831);
    private final CRSUtils crs;

    public PolygonLoader(CRSUtils crs) {
        this.crs = crs;
    }

    public List<PreparedGeometry> loadWktResourceBuffered(String resourcePath, double bufferMeters) throws Exception {
        String wkt = readAll(resourcePath);
        WKTReader reader = new WKTReader();
        Geometry g = reader.read(wkt);
        // Project to metric CRS
        Geometry gMetric = projectGeometryToMetric(g);
        if (bufferMeters > 0) {
            gMetric = gMetric.buffer(bufferMeters);
        }
        List<Geometry> list = new ArrayList<>();
        if (gMetric instanceof GeometryCollection) {
            GeometryCollection gc = (GeometryCollection) gMetric;
            for (int i = 0; i < gc.getNumGeometries(); i++) list.add(gc.getGeometryN(i));
        } else {
            list.add(gMetric);
        }
        return crs.prepareAll(list);
    }

    public List<PreparedGeometry> loadGeoJsonResourceBuffered(String resourcePath, double bufferMeters) throws Exception {
        String json = readAll(resourcePath);
        JSONObject obj = new JSONObject(json);
        List<Geometry> geoms = new ArrayList<>();

        String type = obj.optString("type");
        if ("FeatureCollection".equalsIgnoreCase(type)) {
            JSONArray feats = obj.getJSONArray("features");
            for (int i = 0; i < feats.length(); i++) {
                JSONObject geom = feats.getJSONObject(i).getJSONObject("geometry");
                geoms.addAll(parseGeometryWgs84(geom));
            }
        } else if ("Feature".equalsIgnoreCase(type)) {
            JSONObject geom = obj.getJSONObject("geometry");
            geoms.addAll(parseGeometryWgs84(geom));
        } else {
            geoms.addAll(parseGeometryWgs84(obj));
        }

        List<Geometry> metric = new ArrayList<>();
        for (Geometry g : geoms) {
            Geometry gm = projectGeometryToMetric(g);
            if (bufferMeters > 0) gm = gm.buffer(bufferMeters);
            metric.add(gm);
        }
        return crs.prepareAll(metric);
    }

    private Geometry projectGeometryToMetric(Geometry g) {
        if (g instanceof Polygon) return polygonToMetric((Polygon) g);
        if (g instanceof MultiPolygon) return multiPolygonToMetric((MultiPolygon) g);
        if (g instanceof Point) return crs.toMetric((Point) g);
        if (g instanceof MultiPoint) return multiPointToMetric((MultiPoint) g);
        if (g instanceof LineString) return lineStringToMetric((LineString) g);
        if (g instanceof MultiLineString) return multiLineStringToMetric((MultiLineString) g);
        if (g instanceof GeometryCollection) {
            List<Geometry> parts = new ArrayList<>();
            GeometryCollection gc = (GeometryCollection) g;
            for (int i = 0; i < gc.getNumGeometries(); i++) parts.add(projectGeometryToMetric(gc.getGeometryN(i)));
            return gf25831.createGeometryCollection(parts.toArray(new Geometry[0]));
        }
        return g;
    }

    private Polygon polygonToMetric(Polygon p) {
        LinearRing shell = linearRingToMetric((LinearRing) p.getExteriorRing());
        int nHoles = p.getNumInteriorRing();
        LinearRing[] holes = new LinearRing[nHoles];
        for (int i = 0; i < nHoles; i++) {
            holes[i] = linearRingToMetric((LinearRing) p.getInteriorRingN(i));
        }
        return gf25831.createPolygon(shell, holes);
    }

    private MultiPolygon multiPolygonToMetric(MultiPolygon mp) {
        int n = mp.getNumGeometries();
        Polygon[] polys = new Polygon[n];
        for (int i = 0; i < n; i++) polys[i] = polygonToMetric((Polygon) mp.getGeometryN(i));
        return gf25831.createMultiPolygon(polys);
    }

    private MultiPoint multiPointToMetric(MultiPoint mp) {
        int n = mp.getNumGeometries();
        Point[] pts = new Point[n];
        for (int i = 0; i < n; i++) pts[i] = (Point) projectGeometryToMetric(mp.getGeometryN(i));
        return gf25831.createMultiPoint(pts);
    }

    private LineString lineStringToMetric(LineString ls) {
        Coordinate[] src = ls.getCoordinates();
        Coordinate[] dst = new Coordinate[src.length];
        for (int i = 0; i < src.length; i++) {
            Point p = crs.pointWgs84(src[i].x, src[i].y);
            Point m = crs.toMetric(p);
            dst[i] = new Coordinate(m.getX(), m.getY());
        }
        return gf25831.createLineString(dst);
    }

    private MultiLineString multiLineStringToMetric(MultiLineString mls) {
        int n = mls.getNumGeometries();
        LineString[] arr = new LineString[n];
        for (int i = 0; i < n; i++) arr[i] = lineStringToMetric((LineString) mls.getGeometryN(i));
        return gf25831.createMultiLineString(arr);
    }

    private LinearRing linearRingToMetric(LinearRing lr) {
        Coordinate[] src = lr.getCoordinates();
        Coordinate[] dst = new Coordinate[src.length];
        for (int i = 0; i < src.length; i++) {
            Point p = crs.pointWgs84(src[i].x, src[i].y);
            Point m = crs.toMetric(p);
            dst[i] = new Coordinate(m.getX(), m.getY());
        }
        return gf25831.createLinearRing(dst);
    }

    

    private List<Geometry> parseGeometryWgs84(JSONObject geom) {
        String type = geom.getString("type");
        List<Geometry> list = new ArrayList<>();
        if ("Polygon".equalsIgnoreCase(type)) {
            list.add(polygonFromGeoJson(geom.getJSONArray("coordinates")));
        } else if ("MultiPolygon".equalsIgnoreCase(type)) {
            JSONArray arr = geom.getJSONArray("coordinates");
            for (int i = 0; i < arr.length(); i++) {
                JSONObject tmp = new JSONObject();
                tmp.put("type", "Polygon");
                tmp.put("coordinates", arr.getJSONArray(i));
                list.add(polygonFromGeoJson(arr.getJSONArray(i)));
            }
        }
        // other types not needed for current queries
        return list;
        }

    private Polygon polygonFromGeoJson(JSONArray coords) {
        // coords: [ [ [x,y], ... ] , [ ...holes... ] ] in WGS84
        GeometryFactory gf4326 = new GeometryFactory(new PrecisionModel(), 4326);
        LinearRing shell = ringFromArray(gf4326, coords.getJSONArray(0));
        LinearRing[] holes = new LinearRing[Math.max(0, coords.length() - 1)];
        for (int i = 1; i < coords.length(); i++) holes[i - 1] = ringFromArray(gf4326, coords.getJSONArray(i));
        return gf4326.createPolygon(shell, holes);
    }

    private LinearRing ringFromArray(GeometryFactory gf, JSONArray arr) {
        Coordinate[] pts = new Coordinate[arr.length()];
        for (int i = 0; i < arr.length(); i++) {
            JSONArray c = arr.getJSONArray(i);
            pts[i] = new Coordinate(c.getDouble(0), c.getDouble(1));
        }
        return gf.createLinearRing(pts);
    }

    private String readAll(String resourcePath) throws Exception {
        InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath);
        if (is == null) throw new IllegalArgumentException("Resource not found: " + resourcePath);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) sb.append(line).append('\n');
            return sb.toString();
        }
    }
}
