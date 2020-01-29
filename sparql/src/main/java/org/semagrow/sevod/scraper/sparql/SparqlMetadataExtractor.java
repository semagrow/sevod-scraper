package org.semagrow.sevod.scraper.sparql;

import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.semagrow.sevod.commons.vocabulary.SEVOD;
import org.semagrow.sevod.commons.vocabulary.VOID;

import java.util.List;

/**
 * Created by antonis on 29/7/2016.
 */
public class SparqlMetadataExtractor {

    private String endpoint;
    private String graph;
    private List<String> knownPrefixes;
    private ValueFactory vf = ValueFactoryImpl.getInstance();

    public SparqlMetadataExtractor(String endpoint, String graph, List<String> knownPrefixes) {
        this.endpoint = endpoint;
        this.graph = graph;
        this.knownPrefixes = knownPrefixes;
    }

    public void writeMetadata(RDFWriter writer) throws RDFHandlerException {

        Resource dataset = vf.createBNode();

        writer.handleNamespace(VOID.PREFIX, VOID.NAMESPACE);
        writer.handleNamespace(SEVOD.PREFIX, SEVOD.NAMESPACE);
        writer.handleNamespace(XMLSchema.PREFIX, XMLSchema.NAMESPACE);

        writer.handleStatement(vf.createStatement(dataset, RDF.TYPE, VOID.DATASET));

        for (BindingSet bindings: resultQuery(Queries.predicates)) {

            BNode prop = vf.createBNode();
            URI predicate = getPredicate(bindings);

            writer.handleStatement(vf.createStatement(dataset, VOID.PROPERTYPARTITION, prop));
            writer.handleStatement(vf.createStatement(prop, VOID.PROPERTY, predicate));

            writer.handleStatement(vf.createStatement(prop, VOID.TRIPLES, countQuery(setPredicate(Queries.triples_count, predicate))));
            writer.handleStatement(vf.createStatement(prop, VOID.DISTINCTSUBJECTS, countQuery(setPredicate(Queries.subjects_count, predicate))));
            writer.handleStatement(vf.createStatement(prop, VOID.DISTINCTOBJECTS, countQuery(setPredicate(Queries.objects_count, predicate))));

            for (String prefix: knownPrefixes) {
                if (!askQuery(setPredicate(setPrefix(Queries.subject_prefix, prefix), predicate))) {
                    writer.handleStatement(vf.createStatement(prop, SEVOD.SUBJECTREGEXPATTERN, vf.createLiteral(prefix, XMLSchema.STRING)));
                }
            }

            for (String prefix: knownPrefixes) {
                if (!askQuery(setPredicate(setPrefix(Queries.object_prefix, prefix), predicate))) {
                    writer.handleStatement(vf.createStatement(prop, SEVOD.OBJECTREGEXPATTERN, vf.createLiteral(prefix, XMLSchema.STRING)));
                }
            }
        }

        List<BindingSet> r = resultQuery(Queries.classes);

        for (BindingSet bindings: resultQuery(Queries.classes)) {

            BNode clzp = vf.createBNode();
            URI clazz = getClass(bindings);

            writer.handleStatement(vf.createStatement(dataset, VOID.CLASSPARTITION, clzp));
            writer.handleStatement(vf.createStatement(clzp, VOID.CLASS, clazz));
            writer.handleStatement(vf.createStatement(clzp, VOID.ENTITIES, countQuery(setClass(Queries.entities_count, clazz))));

            for (String prefix: knownPrefixes) {
                if (!askQuery(setClass(setPrefix(Queries.entity_prefix, prefix), clazz))) {
                    writer.handleStatement(vf.createStatement(clzp, SEVOD.SUBJECTREGEXPATTERN, vf.createLiteral(prefix, XMLSchema.STRING)));
                }
            }
        }

        writer.handleStatement(vf.createStatement(dataset, VOID.SPARQLENDPOINT, vf.createURI(endpoint)));
        writer.handleStatement(vf.createStatement(dataset, VOID.TRIPLES, countQuery(Queries.triples_count)));
        writer.handleStatement(vf.createStatement(dataset, VOID.PROPERTIES, countQuery(Queries.predicates_count)));
        writer.handleStatement(vf.createStatement(dataset, VOID.CLASSES, countQuery(Queries.classes_count)));
        writer.handleStatement(vf.createStatement(dataset, VOID.ENTITIES, countQuery(Queries.entities_count)));
        writer.handleStatement(vf.createStatement(dataset, VOID.DISTINCTSUBJECTS, countQuery(Queries.subjects_count)));
        writer.handleStatement(vf.createStatement(dataset, VOID.DISTINCTOBJECTS, countQuery(Queries.objects_count)));
    }

    private List<BindingSet> resultQuery(String query) {
        QueryEvaluator eval = new QueryEvaluator(endpoint);
        return eval.run(setGraph(query));
    }

    private Value countQuery(String query) {
        QueryEvaluator eval = new QueryEvaluator(endpoint);
        return eval.run(setGraph(query)).get(0).getBinding(Queries.count_var.substring(1)).getValue();
    }

    private boolean askQuery(String query) {
        QueryEvaluator eval = new QueryEvaluator(endpoint);
        return eval.run(setGraph(query)).isEmpty();
    }

    private URI getPredicate(BindingSet bindings) {
        return (URI) bindings.getBinding(Queries.predicate_var.substring(1)).getValue();
    }

    private URI getClass(BindingSet bindings) {
        return (URI) bindings.getBinding(Queries.class_var.substring(1)).getValue();
    }

    private String setGraph(String qstr) {
        if (graph != null) {
            return qstr.replace(Queries.graph_var, "<" + graph + ">");
        }
        else {
            return qstr;
        }
    }

    private String setPredicate(String qstr, URI predicate) {
        String rturn = qstr.replace(Queries.predicate_var, "<"+predicate.stringValue()+">");
        return  rturn;
    }

    private String setClass(String qstr, URI clazz) {
        return qstr.replace(Queries.class_var, "<" + clazz.stringValue() + ">");
    }

    private String setPrefix(String qstr, String prefix) {
        return qstr.replace(Queries.prefix_var, "\""+prefix+"\"");
    }
}
