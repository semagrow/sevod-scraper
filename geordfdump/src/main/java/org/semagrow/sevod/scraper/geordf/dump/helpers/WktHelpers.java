package org.semagrow.sevod.scraper.geordf.dump.helpers;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.ValueFactoryImpl;
import org.semagrow.sevod.scraper.geordf.dump.vocabulary.GEO;

public final class WktHelpers {

    private static WKTReader wktReader;
    private static WKTWriter wktWriter;

    private static ValueFactory vf;

    static {
        wktReader = new WKTReader();
        wktWriter = new WKTWriter();
        vf = ValueFactoryImpl.getInstance();
    }

    public static final IRI getCRS(Literal l) {
        assert l.getDatatype().equals(GEO.WKT_LITERAL);

        String str = l.stringValue();

        if (str.startsWith("<")) {
            int n = str.indexOf(">");
            return vf.createIRI(str.substring(1,n));
        }
        else {
            return vf.createIRI(GEO.DEFAULT_SRID);
        }
    }

    public static final Geometry createGeometry(Literal l, IRI crs) throws ParseException {
        assert l.getDatatype().equals(GEO.WKT_LITERAL);
        String str = l.stringValue();

        if (str.startsWith("<")) {
            if (getCRS(l).equals(crs)) {
                int n = str.indexOf(">") + 2;
                return wktReader.read(str.substring(n));
            }
        }
        else {
            if (crs.equals(GEO.DEFAULT_SRID)) {
                return wktReader.read(str);
            }
        }
        throw new ParseException("Non matching CRS");
    }


    public static final Literal createWKTLiteral(Geometry geometry, IRI crs) {

        String wktStr = wktWriter.write(geometry);
        String crsStr = crs.equals(GEO.DEFAULT_SRID) ? "" : "<" + crs.toString() + "> ";
        Literal l = vf.createLiteral(crsStr + wktStr, GEO.WKT_LITERAL);

        return l;
    }
}
