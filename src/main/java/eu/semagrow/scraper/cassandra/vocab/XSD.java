package eu.semagrow.scraper.cassandra.vocab;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Created by antonis on 7/6/2016.
 */
public class XSD {

    public static final String NAMESPACE = "http://www.w3.org/2001/XMLSchema#";

    public static final String PREFIX = "xsd";

    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    public final static URI STRING;
    public final static URI INTEGER;
    public final static URI DECIMAL;
    public final static URI INT;
    public final static URI LONG;
    public final static URI DOUBLE;
    public final static URI FLOAT;
    public final static URI BOOLEAN;

    static {
        ValueFactory factory = ValueFactoryImpl.getInstance();
        STRING = factory.createURI(XSD.NAMESPACE, "string");
        INTEGER = factory.createURI(XSD.NAMESPACE, "integer");
        DECIMAL = factory.createURI(XSD.NAMESPACE, "decimal");
        INT = factory.createURI(XSD.NAMESPACE, "int");
        LONG = factory.createURI(XSD.NAMESPACE, "long");
        DOUBLE = factory.createURI(XSD.NAMESPACE, "double");
        FLOAT = factory.createURI(XSD.NAMESPACE, "float");
        BOOLEAN = factory.createURI(XSD.NAMESPACE, "boolean");
    }
}
