package org.semagrow.sevod.scraper.geordf.dump.metadata;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.ValueFactoryImpl;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.semagrow.sevod.commons.vocabulary.SEVOD;
import org.semagrow.sevod.scraper.geordf.dump.helpers.WktHelpers;
import org.semagrow.sevod.scraper.geordf.dump.vocabulary.GEO;
import org.semagrow.sevod.scraper.rdf.dump.metadata.Metadata;

public class BoundingPolygonMetadata implements Metadata {

    private ValueFactory vf = ValueFactoryImpl.getInstance();

    private BoundingPolygon boundingPolygon = new BoundingPolygon();
    private IRI crs = null;
    private int approximation_depth;

    public BoundingPolygonMetadata() {
        approximation_depth = 1;
    }

    public BoundingPolygonMetadata(String type) {
        if (type.equals("mbb")) {
            approximation_depth = 1;
        }
        if (type.equals("union")) {
            approximation_depth = Integer.MAX_VALUE;
        }
        if (type.startsWith("qt")) {
            approximation_depth = Integer.parseInt(type.substring(2));
        }
    }

    @Override
    public void processStatement(Statement statement) {

        Value o = statement.getObject();

        if (o instanceof Literal
                && (((Literal) o).getDatatype() != null)
                && ((Literal) o).getDatatype().equals(GEO.WKT_LITERAL)) {

            try {
                if (crs == null) {
                    crs = WktHelpers.getCRS((Literal) o);
                }

                Geometry g = WktHelpers.createGeometry((Literal) o, crs);
                boundingPolygon.extend(g);

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void serializeMetadata(Resource dataset, RDFWriter writer) throws RDFHandlerException {
        Geometry b = null;

        if (approximation_depth == Integer.MAX_VALUE) {
            b = boundingPolygon.getUnion();
        }
        else if (approximation_depth == 1) {
            b = boundingPolygon.getEnvelope();
        }
        else {
            b = boundingPolygon.getQuadTreeApproximation(approximation_depth);
        }

        writer.handleStatement(vf.createStatement(dataset, SEVOD.BOUNDINGWKT, WktHelpers.createWKTLiteral(b, crs)));
    }
}
