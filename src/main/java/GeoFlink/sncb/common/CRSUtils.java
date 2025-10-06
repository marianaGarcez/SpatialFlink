package GeoFlink.sncb.common;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.proj4j.CRSFactory;
import org.locationtech.proj4j.CoordinateReferenceSystem;
import org.locationtech.proj4j.CoordinateTransform;
import org.locationtech.proj4j.CoordinateTransformFactory;
import org.locationtech.proj4j.ProjCoordinate;

import java.util.ArrayList;
import java.util.List;

public class CRSUtils {

    private final GeometryFactory gf4326 = new GeometryFactory(new PrecisionModel(), 4326);
    private final GeometryFactory gf25831 = new GeometryFactory(new PrecisionModel(), 25831);
    private final CoordinateTransform toMetric;

    public CRSUtils() {
        CRSFactory crsFactory = new CRSFactory();
        CoordinateReferenceSystem src = crsFactory.createFromName("EPSG:4326");
        CoordinateReferenceSystem dst = crsFactory.createFromName("EPSG:25831");
        CoordinateTransformFactory ctf = new CoordinateTransformFactory();
        this.toMetric = ctf.createTransform(src, dst);
    }

    public Point pointWgs84(double lon, double lat) {
        return gf4326.createPoint(new Coordinate(lon, lat));
    }

    public Point toMetric(Point wgs84Point) {
        ProjCoordinate src = new ProjCoordinate(wgs84Point.getX(), wgs84Point.getY());
        ProjCoordinate dst = new ProjCoordinate();
        toMetric.transform(src, dst);
        return gf25831.createPoint(new Coordinate(dst.x, dst.y));
    }

    public List<PreparedGeometry> prepareAll(List<Geometry> geoms) {
        List<PreparedGeometry> out = new ArrayList<>();
        PreparedGeometryFactory pf = new PreparedGeometryFactory();
        for (Geometry g : geoms) {
            out.add(pf.create(g));
        }
        return out;
    }

    public Geometry bufferMeters(Geometry metricGeom, double meters) {
        return metricGeom.buffer(meters);
    }
}

