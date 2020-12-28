package org.semagrow.sevod.scraper.cql.vocab;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.NamespaceImpl;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Created by antonis on 7/6/2016.
 */
public class CDT {

    public static final String NAMESPACE = "http://iit.demokritos.gr/WKT/cassandraDatatype#";

    public static final String PREFIX = "cdt";

    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);


    public final static IRI BLOB;
    public final static IRI UDT;
    public final static IRI LIST;
    public final static IRI SET;
    public final static IRI MAP;
    public final static IRI DATE;
    public final static IRI TIME;
    public final static IRI TIMESTAMP;
    public final static IRI TIMEUUID;
    public final static IRI TUPLE;

    static {
        ValueFactory factory = SimpleValueFactory.getInstance();
        BLOB = factory.createIRI(CDT.NAMESPACE, "blob");
        UDT = factory.createIRI(CDT.NAMESPACE, "udt");
        LIST = factory.createIRI(CDT.NAMESPACE, "list");
        SET = factory.createIRI(CDT.NAMESPACE, "set");
        MAP = factory.createIRI(CDT.NAMESPACE, "map");
        DATE = factory.createIRI(CDT.NAMESPACE, "date");
        TIME = factory.createIRI(CDT.NAMESPACE, "time");
        TIMESTAMP = factory.createIRI(CDT.NAMESPACE, "timestamp");
        TIMEUUID = factory.createIRI(CDT.NAMESPACE, "timeuuid");
        TUPLE = factory.createIRI(CDT.NAMESPACE, "tuple");
    }
}
