package org.semagrow.sevod.scraper.geordf.dump.metadata;

import org.locationtech.jts.geom.Geometry;

public class BoundingPolygonApprox extends BoundingPolygonBase implements BoundingPolygon {

    private int depth;

    public BoundingPolygonApprox(int depth) {
        this.depth = depth;
    }

    @Override
    public Geometry serialize() {
        return super.getQuadTreeApproximation(depth);
    }
}
