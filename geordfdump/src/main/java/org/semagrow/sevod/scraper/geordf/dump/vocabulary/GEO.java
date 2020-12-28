package org.semagrow.sevod.scraper.geordf.dump.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public final class GEO {

    public final static String NAMESPACE = "http://www.opengis.net/ont/geosparql#";
    public final static String PREFIX = "geo";

    public final static IRI WKT_LITERAL;
    public final static IRI HAS_GEOMETRY;
    public final static IRI AS_WKT;
    
    public static final String DEFAULT_SRID = "http://www.opengis.net/def/crs/OGC/1.3/CRS84";


    static {
        ValueFactory vf = SimpleValueFactory.getInstance();

        WKT_LITERAL = vf.createIRI(NAMESPACE + "wktLiteral");
        HAS_GEOMETRY = vf.createIRI(NAMESPACE + "hasGeometry");
        AS_WKT = vf.createIRI(NAMESPACE + "asWKT");
    }

}
