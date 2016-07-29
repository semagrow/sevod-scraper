package org.semagrow.sevod.scraper.sparql;

import org.openrdf.model.vocabulary.RDF;

/**
 * Created by antonis on 29/7/2016.
 */
public final class VoidQueries {

    public final static String triples;
    public final static String subjects;
    public final static String predicates;
    public final static String objects;
    public final static String classes;
    public final static String entities;

    public final static String predicate_triples;
    public final static String predicate_subjects;
    public final static String predicate_objects;

    public final static String class_entities;

    public final static String predicate_var;
    public final static String class_var;
    public final static String count_var;

    static {
        triples    = "select count(*) as ?no where { ?s ?p ?o }";
        subjects   = "select count(distinct ?s) as ?no where { ?s ?p ?o }";
        predicates = "select count(distinct ?p) as ?no where { ?s ?p ?o }";
        objects    = "select count(distinct ?o) as ?no where { ?s ?p ?o }";
        classes    = "select count(distinct ?o) as ?no where { ?s " + RDF.TYPE + " ?o }";
        entities   = "select count(distinct ?s) as ?no where { ?s " + RDF.TYPE + " ?o }";

        predicate_triples  = "select ?p (count(?s) AS ?no) { ?s ?p ?o } group by ?p";
        predicate_subjects = "select ?p (count(distinct ?s) AS ?no) { ?s ?p ?o } group by ?p";
        predicate_objects  = "select ?p (count(distinct ?o) AS ?no) { ?s ?p ?o } group by ?p";

        class_entities = "select ?o (count(distinct ?s) AS ?no) { ?s " + RDF.TYPE + " ?o } group by ?o";

        predicate_var = "?p";
        class_var = "?o";
        count_var = "?no";
    }

}
