package org.semagrow.sevod.scraper.geordf.dump.metadata;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.semagrow.sevod.scraper.geordf.dump.helpers.WktHelpers;
import org.semagrow.sevod.scraper.geordf.dump.vocabulary.GEO;
import org.semagrow.sevod.scraper.geordf.dump.vocabulary.VOCAB;
import org.semagrow.sevod.scraper.rdf.dump.metadata.Metadata;

public class BoundingBoxMetadata implements Metadata {

    private ValueFactory vf = ValueFactoryImpl.getInstance();

    private Geometry mbb = null;
    private URI crs = null;

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

                if (mbb == null) {
                    mbb = g.getEnvelope();
                }
                else {
                    mbb = g.getEnvelope().union(mbb).getEnvelope();
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void serializeMetadata(Resource dataset, RDFWriter writer) throws RDFHandlerException {
        if (mbb != null) {
            writer.handleStatement(vf.createStatement(dataset, VOCAB.DATASET_BOUNDING_POLYGON, WktHelpers.createWKTLiteral(mbb, crs)));
        }
    }
}