package org.semagrow.sevod.scraper.geordf.dump.vocabulary;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.semagrow.sevod.commons.vocabulary.SEVOD;

public final class GEO {

    public final static String NAMESPACE = "http://www.opengis.net/ont/geosparql#";
    public final static String PREFIX = "geo";

    public final static URI WKT_LITERAL;
    public final static URI HAS_GEOMETRY;
    public final static URI AS_WKT;
    
    public static final String DEFAULT_SRID = "http://www.opengis.net/def/crs/OGC/1.3/CRS84";


    static {
        ValueFactory vf = ValueFactoryImpl.getInstance();

        WKT_LITERAL = vf.createURI(NAMESPACE + "wktLiteral");
        HAS_GEOMETRY = vf.createURI(NAMESPACE + "hasGeometry");
        AS_WKT = vf.createURI(NAMESPACE + "asWKT");
    }

}
