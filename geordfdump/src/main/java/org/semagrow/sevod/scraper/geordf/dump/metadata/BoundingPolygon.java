package org.semagrow.sevod.scraper.geordf.dump.metadata;

import org.locationtech.jts.geom.Geometry;

public interface BoundingPolygon {
    void extend(Geometry g);
    Geometry serialize();
}
