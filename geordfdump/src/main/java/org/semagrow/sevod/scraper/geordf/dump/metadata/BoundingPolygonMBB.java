package org.semagrow.sevod.scraper.geordf.dump.metadata;

import org.locationtech.jts.geom.Geometry;

public class BoundingPolygonMBB extends BoundingPolygonBase implements BoundingPolygon {
    @Override
    public Geometry serialize() {
        return super.getEnvelope();
    }
}
