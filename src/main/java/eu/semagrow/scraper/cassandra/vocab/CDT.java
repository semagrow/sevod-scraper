package eu.semagrow.scraper.cassandra.vocab;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Created by antonis on 7/6/2016.
 */
public class CDT {

    public static final String NAMESPACE = "http://iit.demokritos.gr/WKT/cassandraDatatype#";

    public static final String PREFIX = "cdt";

    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);


    public final static URI BLOB;
    public final static URI UDT;
    public final static URI LIST;
    public final static URI SET;
    public final static URI MAP;
    public final static URI DATE;
    public final static URI TIME;
    public final static URI TIMESTAMP;
    public final static URI TIMEUUID;
    public final static URI TUPLE;

    static {
        ValueFactory factory = ValueFactoryImpl.getInstance();
        BLOB = factory.createURI(CDT.NAMESPACE, "blob");
        UDT = factory.createURI(CDT.NAMESPACE, "udt");
        LIST = factory.createURI(CDT.NAMESPACE, "list");
        SET = factory.createURI(CDT.NAMESPACE, "set");
        MAP = factory.createURI(CDT.NAMESPACE, "map");
        DATE = factory.createURI(CDT.NAMESPACE, "date");
        TIME = factory.createURI(CDT.NAMESPACE, "time");
        TIMESTAMP = factory.createURI(CDT.NAMESPACE, "timestamp");
        TIMEUUID = factory.createURI(CDT.NAMESPACE, "timeuuid");
        TUPLE = factory.createURI(CDT.NAMESPACE, "tuple");
    }
}
