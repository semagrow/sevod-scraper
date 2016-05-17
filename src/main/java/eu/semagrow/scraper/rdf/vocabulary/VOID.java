package eu.semagrow.scraper.rdf.vocabulary;

/**
 * Created by antonis on 21/4/2015.
 */

public enum VOID {

    // concepts
    Dataset,
    Linkset,
    EquiWidthHist,
    EuqiDepthHist,

    // predicates
    vocabulary,
    sparqlEndpoint,
    distinctSubjects,
    distinctObjects,
    triples,
    classes,
    entities,
    properties,
    classPartition,
    clazz("class"),
    propertyPartition,
    property,
    target,
    subset,
    linkPredicate,

    histogram,
    minValue,
    maxValue,
    bucketLoad,
    buckets,
    bucketDef;

    public static final String NAMESPACE = "http://rdfs.org/ns/void#";

    private final String uri;

    private VOID(String name) {
        this.uri = NAMESPACE + name;
    }

    private VOID() {
        this.uri = NAMESPACE + super.toString();
    }

    public String toString() {
        return this.uri;
    }

}