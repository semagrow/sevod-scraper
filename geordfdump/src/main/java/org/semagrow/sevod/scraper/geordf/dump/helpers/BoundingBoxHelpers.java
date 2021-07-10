package org.semagrow.sevod.scraper.geordf.dump.helpers;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.ArrayList;
import java.util.List;

public final class BoundingBoxHelpers {

    private static final GeometryFactory gf;

    static {
        gf = new GeometryFactory();
    }

    public static List<Geometry> partitionBoundingBox(Geometry bb, int Xs, int Ys) {

        Coordinate[] coordinates = bb.getCoordinates();

        double min_x = coordinates[0].getX();
        double max_x = coordinates[0].getX();
        double min_y = coordinates[0].getY();
        double max_y = coordinates[0].getY();

        for (Coordinate coord: bb.getCoordinates()) {

            if (coord.getX() < min_x) {
                min_x = coord.getX();
            }
            if (coord.getX() > max_x) {
                max_x = coord.getX();
            }
            if (coord.getY() < min_y) {
                min_y = coord.getY();
            }
            if (coord.getY() > max_y) {
                max_y = coord.getY();
            }
        }

        double xFactor = (max_x - min_x)/Xs;
        double yFactor = (max_y - min_y)/Ys;

        List<Geometry> result = new ArrayList<Geometry>();

        for (int i=0; i<Xs; i++) {
            for (int j=0; j<Ys; j++) {
                Coordinate[] p = new Coordinate[5];

                p[0] = new Coordinate(i*xFactor + min_x, j*yFactor + min_y);
                p[1] = new Coordinate((i+1)*xFactor + min_x, j*yFactor + min_y);
                p[2] = new Coordinate((i+1)*xFactor + min_x, (j+1)*yFactor + min_y);
                p[3] = new Coordinate(i*xFactor + min_x, (j+1)*yFactor + min_y);
                p[4] = new Coordinate(i*xFactor + min_x, j*yFactor + min_y);

                result.add(gf.createPolygon(p));
            }
        }

        return result;
    }
}

