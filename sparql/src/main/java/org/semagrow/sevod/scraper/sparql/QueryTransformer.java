package org.semagrow.sevod.scraper.sparql;

import org.eclipse.rdf4j.model.IRI;

public class QueryTransformer {

    private String qStr = "";

    public QueryTransformer from(String qStr) {
        this.qStr = qStr;
        return this;
    }

    public QueryTransformer setGraph(String graph) {
        if (graph != null) {
            qStr = qStr.replace(Queries.graph_var, "<" + graph + ">");
        }
        return this;
    }

    public QueryTransformer setPredicate(IRI predicate) {
        if (predicate != null) {
            qStr = qStr.replace(Queries.predicate_var, "<"+predicate.stringValue()+">");
        }
        return this;
    }

    public QueryTransformer setClass(IRI clazz) {
        if (clazz != null) {
            qStr = qStr.replace(Queries.class_var, "<" + clazz.stringValue() + ">");
        }
        return this;
    }

    public QueryTransformer setPrefix(String prefix) {
        if (prefix != null) {
            qStr = qStr.replace(Queries.prefix_var, "\""+prefix+"\"");
        }
        return this;
    }

    public String toString() {
        return qStr;
    }
}
