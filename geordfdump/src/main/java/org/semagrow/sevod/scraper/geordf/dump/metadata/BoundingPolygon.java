package org.semagrow.sevod.scraper.geordf.dump.metadata;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.semagrow.sevod.scraper.geordf.dump.helpers.BoundingBoxHelpers;

import java.util.ArrayList;
import java.util.List;

public class BoundingPolygon {

    private final GeometryFactory gf = new GeometryFactory();

    private Geometry union = gf.createPolygon();

    public void extend(Geometry g) {
        if (union.isEmpty()) {
            union = g.buffer(0);
        }
        else {
            union = g.buffer(0).union(union).buffer(0);
        }
    }

    public Geometry getUnion() {
        return union;
    }

    public Geometry getEnvelope() {
        return union.getEnvelope();
    }

    public Geometry getQuadTreeApproximation(int depth) {
        List<Geometry> boxes = new ArrayList<>();
        boxes.add(union.getEnvelope());

        while (depth > 0) {
            List<Geometry> new_boxes = new ArrayList<>();
            for (Geometry b: boxes) {
                if (b.within(union)) {
                    new_boxes.add(b);
                }
                else {
                    new_boxes.addAll(BoundingBoxHelpers.partitionBoundingBox(b, 4, 4));
                }
            }
            boxes = new ArrayList<>();
            for (Geometry b : new_boxes) {
                if (!b.disjoint(union)) {
                    boxes.add(b);
                }
            }
            depth--;
        }

        Geometry approx = boxes.get(0);
        for (Geometry b : boxes) {
            approx = b.union(approx);
        }
        return approx;
    }
}