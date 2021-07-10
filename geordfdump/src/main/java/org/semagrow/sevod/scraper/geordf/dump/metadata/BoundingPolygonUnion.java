package org.semagrow.sevod.scraper.geordf.dump.metadata;

import org.locationtech.jts.geom.Geometry;

public class BoundingPolygonUnion extends BoundingPolygonBase implements BoundingPolygon {
    @Override
    public Geometry serialize() {
        return super.getUnion();
    }
}
