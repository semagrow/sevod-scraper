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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KnownPolygonMetadata implements Metadata {

    final private Logger log = LoggerFactory.getLogger(KnownPolygonMetadata.class);

    private ValueFactory vf = ValueFactoryImpl.getInstance();

    private Geometry mbp;
    private IRI crs = null;

    public KnownPolygonMetadata(Geometry mbp) {
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
            writer.handleStatement(vf.createStatement(dataset, SEVOD.BOUNDINGWKT, WktHelpers.createWKTLiteral(mbp, crs)));
        }
    }
}
