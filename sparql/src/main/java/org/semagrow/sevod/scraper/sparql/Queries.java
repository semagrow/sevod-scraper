package org.semagrow.sevod.scraper.sparql;

/**
 * Created by antonis on 29/7/2016.
 */
public final class Queries {

    public final static String triples_count;
    public final static String subjects_count;
    public final static String predicates_count;
    public final static String objects_count;
    public final static String classes_count;
    public final static String entities_count;

    public final static String predicates;
    public final static String classes;

    public final static String subject_prefix;
    public final static String object_prefix;
    public final static String entity_prefix;

    public final static String graph_var;
    public final static String predicate_var;
    public final static String class_var;
    public final static String prefix_var;
    public final static String count_var;

    static {

        triples_count    = "select (count(*) as ?c) where { graph ?g { ?s ?p ?o } }";
        subjects_count   = "select (count(distinct ?s) as ?c) where { graph ?g { ?s ?p ?o } }";
        predicates_count = "select (count(distinct ?p) as ?c) where { graph ?g { ?s ?p ?o } }";
        objects_count    = "select (count(distinct ?o) as ?c) where { graph ?g { ?s ?p ?o } }";
        classes_count    = "select (count(distinct ?o) as ?c) where { graph ?g { ?s a ?o } }";
        entities_count   = "select (count(distinct ?s) as ?c) where { graph ?g { ?s a ?o } }";

        predicates = "select distinct ?p where { graph ?g { ?s ?p ?o } }";
        classes    = "select distinct ?o where { graph ?g { ?s a ?o } }";

        subject_prefix = "select * where { graph ?g { ?s ?p ?o . filter(strstarts(str(?s), ?z)) } } LIMIT 1";
        object_prefix  = "select * where { graph ?g { ?s ?p ?o . filter(strstarts(str(?o), ?z)) } } LIMIT 1";
        entity_prefix  = "select * where { graph ?g { ?s a ?o . filter(strstarts(str(?s), ?z)) } } LIMIT 1";

        graph_var = "?g";
        predicate_var = "?p";
        class_var = "?o";
        prefix_var = "?z";
        count_var = "?c";

    }
}
