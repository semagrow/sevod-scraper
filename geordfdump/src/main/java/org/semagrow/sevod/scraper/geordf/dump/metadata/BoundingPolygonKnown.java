package org.semagrow.sevod.scraper.geordf.dump.metadata;

import org.locationtech.jts.geom.Geometry;

public class BoundingPolygonKnown implements BoundingPolygon {

    private Geometry boundingPolygon;

    public BoundingPolygonKnown(Geometry boundingPolygon) {
        this.boundingPolygon = boundingPolygon;
    }

    @Override
    public void extend(Geometry g) {

    }

    @Override
    public Geometry serialize() {
        return null;
    }
}
