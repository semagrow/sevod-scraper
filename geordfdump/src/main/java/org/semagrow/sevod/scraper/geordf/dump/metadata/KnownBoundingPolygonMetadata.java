package org.semagrow.sevod.scraper.geordf.dump.metadata;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.semagrow.sevod.scraper.geordf.dump.helpers.WktHelpers;
import org.semagrow.sevod.scraper.geordf.dump.vocabulary.GEO;
import org.semagrow.sevod.scraper.geordf.dump.vocabulary.VOCAB;
import org.semagrow.sevod.scraper.rdf.dump.MetadataGenerator;
import org.semagrow.sevod.scraper.rdf.dump.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KnownBoundingPolygonMetadata implements Metadata {

    final private Logger log = LoggerFactory.getLogger(KnownBoundingPolygonMetadata.class);

    private ValueFactory vf = ValueFactoryImpl.getInstance();

    private Geometry mbp;
    private URI crs = null;

    public KnownBoundingPolygonMetadata(Geometry mbp) {
        this.mbp = mbp;
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

                if (!mbp.contains(g)) {
                    log.info(g.toString() + " is not contained in " + mbp.toString());
                    mbp = null;
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void serializeMetadata(Resource dataset, RDFWriter writer) throws RDFHandlerException {
        if (mbp != null) {
            writer.handleStatement(vf.createStatement(dataset, VOCAB.DATASET_BOUNDING_POLYGON, WktHelpers.createWKTLiteral(mbp, crs)));
        }
    }
}
