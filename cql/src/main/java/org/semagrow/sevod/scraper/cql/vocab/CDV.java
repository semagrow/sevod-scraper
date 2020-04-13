package org.semagrow.sevod.scraper.cql.vocab;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Created by antonis on 7/4/2016.
 */
public final class CDV {
    public static final String NAMESPACE = "http://schema.org/";

    public static final IRI CASSANDRADB;
    public static final IRI SCHEMA;

    public static final IRI ADDRESS;
    public static final IRI PORT;
    public static final IRI KEYSPACE;

    public static final IRI BASE;

    public static final IRI TABLES;
    public static final IRI NAME;
    public static final IRI TABLESCHEMA;
    public static final IRI PRIMARYKEY;
    public static final IRI SECONDARYINDEX;

    public static final IRI COLUMNS;
    public static final IRI COLUMNTYPE;
    public static final IRI PARTITION;
    public static final IRI CLUSTERING;
    public static final IRI REGULAR;
    public static final IRI CLUSTERINGPOSITION;
    public static final IRI CLUSTERINGORDER;
    public static final IRI DATATYPE;

    static {
        ValueFactory vf = SimpleValueFactory.getInstance();

        CASSANDRADB = vf.createIRI(NAMESPACE, "cassandraDB");
        SCHEMA = vf.createIRI(NAMESPACE, "cassandraSchema");

        ADDRESS = vf.createIRI(NAMESPACE, "address");
        PORT = vf.createIRI(NAMESPACE, "port");
        KEYSPACE = vf.createIRI(NAMESPACE, "keyspace");

        BASE = vf.createIRI(NAMESPACE, "base");

        TABLES = vf.createIRI(NAMESPACE, "tables");
        NAME = vf.createIRI(NAMESPACE, "name");
        TABLESCHEMA = vf.createIRI(NAMESPACE, "tableSchema");
        PRIMARYKEY = vf.createIRI(NAMESPACE, "primaryKey");
        SECONDARYINDEX = vf.createIRI(NAMESPACE, "secondaryIndex");

        COLUMNS = vf.createIRI(NAMESPACE, "columns");
        COLUMNTYPE = vf.createIRI(NAMESPACE, "columnType");
        PARTITION = vf.createIRI(NAMESPACE, "partition");
        CLUSTERING = vf.createIRI(NAMESPACE, "clustering");
        REGULAR = vf.createIRI(NAMESPACE, "regular");
        CLUSTERINGPOSITION = vf.createIRI(NAMESPACE, "clusteringPosition");
        CLUSTERINGORDER = vf.createIRI(NAMESPACE, "clusteringOrder");
        DATATYPE = vf.createIRI(NAMESPACE, "datatype");
    }
}
