package org.semagrow.sevod.scraper.geordf.dump.metadata;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class BoundingPolygonEmpty implements BoundingPolygon {

    @Override
    public void extend(Geometry g) {

    }

    @Override
    public Geometry serialize() {
        return new GeometryFactory().createPolygon();
    }
}
