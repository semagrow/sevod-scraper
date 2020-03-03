package org.semagrow.sevod.scraper.geordf.dump.metadata;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.semagrow.sevod.scraper.geordf.dump.vocabulary.GEO;
import org.semagrow.sevod.scraper.geordf.dump.vocabulary.VOCAB;
import org.semagrow.sevod.scraper.rdf.dump.metadata.Metadata;

import java.util.HashSet;
import java.util.Set;

public class BoundingBoxMetadata implements Metadata {

    private ValueFactory vf = ValueFactoryImpl.getInstance();

    private Geometry mbb = null;

    private final WKTReader wktReader = new WKTReader();
    private final WKTWriter wktWriter = new WKTWriter();

    private Set<URI> hasGeomProps; // "synonyms" for geosparql:hasGeometry
    private Set<URI> asWktProps; // "synonnyms" for geosparql:asWKT

    private Set<URI> geometricURIs = new HashSet<>(); // all subjects of geosparql:hasGeometry and geosparql:asWKT.
    // we assume that every subject of hasGeometry has one geometry defined in this dump file.

    public BoundingBoxMetadata(Set<URI> hasGeomProps, Set<URI> asWktProps) {
        this.hasGeomProps = hasGeomProps;
        this.asWktProps = asWktProps;
        hasGeomProps.add(GEO.HAS_GEOMETRY);
        asWktProps.add(GEO.AS_WKT);
    }

    public void serializePreamble(RDFWriter writer) throws RDFHandlerException {
        writer.handleNamespace(RDFS.PREFIX, RDFS.NAMESPACE);
        writer.handleNamespace(GEO.PREFIX, GEO.NAMESPACE);

        for(URI p: hasGeomProps) {
            if (!p.equals(GEO.HAS_GEOMETRY)) {
                writer.handleStatement(vf.createStatement(p, RDFS.SUBPROPERTYOF, GEO.HAS_GEOMETRY));
            }
        }
        for(URI p: asWktProps) {
            if (!p.equals(GEO.AS_WKT)) {
                writer.handleStatement(vf.createStatement(p, RDFS.SUBPROPERTYOF, GEO.AS_WKT));
            }
        }
    }

    @Override
    public void processStatement(Statement statement) {

        Value o = statement.getObject();

        if (o instanceof Literal
                && (((Literal) o).getDatatype() != null)
                && ((Literal) o).getDatatype().equals(GEO.WKT_LITERAL)) {

            try {
                Geometry g = parseWKTLiteral((Literal) o);

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

        for (URI p: hasGeomProps) {
            if (statement.getPredicate().equals(p)) {
                geometricURIs.add((URI) statement.getSubject());
            }
        }

        for (URI p: asWktProps) {
            if (statement.getPredicate().equals(p)) {
                geometricURIs.add((URI) statement.getSubject());
            }
        }
    }

    @Override
    public void serializeMetadata(Resource dataset, RDFWriter writer) throws RDFHandlerException {
        assert mbb != null;

        Resource mbbResource = vf.createBNode();
        Resource geometry = vf.createBNode();

        writer.handleStatement(vf.createStatement(dataset, VOCAB.DATASET_BOUNDING_BOX, mbbResource));
        writer.handleStatement(vf.createStatement(mbbResource, GEO.HAS_GEOMETRY, geometry));
        writer.handleStatement(vf.createStatement(geometry, GEO.AS_WKT, createWKTLiteral(mbb)));
    }

    public boolean isGeometricURI(URI uri) {
        return geometricURIs.contains(uri);
    }

    private Geometry parseWKTLiteral(Literal wktLiteral) throws ParseException {

        String wktString = wktLiteral.stringValue();

        if (wktString.startsWith("<")) {
            int n = wktString.indexOf(">");
            URI srid = vf.createURI(wktString.substring(1,n));

            assert srid.equals(VOCAB.DEFAULT_CRS);

            wktString = wktString.substring(n+2);
        }

        return wktReader.read(wktString);
    }


    private Literal createWKTLiteral(Geometry geometry) {

        String wktStr = wktWriter.write(geometry);
        String sridStr = "<" + VOCAB.DEFAULT_CRS.toString() + "> ";
        Literal literal = vf.createLiteral(sridStr + wktStr, GEO.WKT_LITERAL);

        return literal;
    }
}
