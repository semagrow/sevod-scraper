package org.semagrow.sevod.scraper.rdf.dump.metadata;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.semagrow.sevod.commons.vocabulary.SEVOD;

public class BoundaryMetadata implements Metadata {

    private ValueFactory vf = ValueFactoryImpl.getInstance();

    private Geometry mbb = null;
    private URI srid = null;

    private final URI wktLiteral = vf.createURI("http://www.opengis.net/ont/geosparql#wktLiteral");
    private final URI svdDatasetBoundary = vf.createURI(SEVOD.NAMESPACE + "datasetBoundingBox");

    private final WKTReader wktReader = new WKTReader();
    private final WKTWriter wktWriter = new WKTWriter();

    @Override
    public void processStatement(Statement statement) {

        Value o = statement.getObject();

        if (o instanceof Literal
                && (((Literal) o).getDatatype() != null)
                && ((Literal) o).getDatatype().equals(wktLiteral)) {

            String wktString = o.stringValue();

            if (wktString.startsWith("<")) {
                int n = wktString.indexOf(">");
                srid = vf.createURI(wktString.substring(1,n));
                wktString = wktString.substring(n+2);
            }

            try {
                Geometry g = wktReader.read(wktString);

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
            String wktStr = wktWriter.write(mbb);
            String sridStr = (srid == null) ? "" : "<" + srid.toString() + "> ";
            Literal literal = vf.createLiteral(sridStr + wktStr, wktLiteral);
            writer.handleStatement(vf.createStatement(dataset, svdDatasetBoundary, literal));
        }
    }
}
