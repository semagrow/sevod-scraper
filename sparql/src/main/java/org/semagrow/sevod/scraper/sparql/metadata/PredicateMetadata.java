package org.semagrow.sevod.scraper.sparql.metadata;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.semagrow.sevod.commons.vocabulary.SEVOD;
import org.semagrow.sevod.commons.vocabulary.VOID;
import org.semagrow.sevod.scraper.sparql.Queries;
import org.semagrow.sevod.scraper.sparql.QueryEvaluator;
import org.semagrow.sevod.scraper.sparql.QueryTransformer;

import java.util.HashSet;
import java.util.Set;

public class PredicateMetadata implements Metadata {

    private ValueFactory vf = SimpleValueFactory.getInstance();

    private IRI predicate;
    private String graph;
    private Set<String> knownPrefixes;

    private int triples = 0;
    private int subjCount = 0;
    private int objCount = 0;

    private Set<String> subjPrefix = new HashSet<>();
    private Set<String> objPrefix = new HashSet<>();

    public PredicateMetadata(IRI predicate, String graph, Set<String> knownPrefixes) {
        this.predicate = predicate;
        this.graph = graph;
        this.knownPrefixes = knownPrefixes;
    }

    @Override
    public void processEndpoint(String endpoint) {

        QueryEvaluator eval = new QueryEvaluator(endpoint);
        QueryTransformer qt = new QueryTransformer();

        triples = eval.count(qt.from(Queries.triples_count).setPredicate(predicate).setGraph(graph).toString());
        subjCount = eval.count(qt.from(Queries.subjects_count).setPredicate(predicate).setGraph(graph).toString());
        objCount = eval.count(qt.from(Queries.objects_count).setPredicate(predicate).setGraph(graph).toString());

        for (String prefix: knownPrefixes) {
            if (eval.ask(qt.from(Queries.subject_prefix).setPredicate(predicate).setGraph(graph).setPrefix(prefix).toString())) {
                subjPrefix.add(prefix);
            }
        }
        for (String prefix: knownPrefixes) {
            if (eval.ask(qt.from(Queries.object_prefix).setPredicate(predicate).setGraph(graph).setPrefix(prefix).toString())) {
                objPrefix.add(prefix);
            }
        }
    }

    @Override
    public void serializeMetadata(Resource dataset, RDFWriter writer) throws RDFHandlerException {
        BNode prop = vf.createBNode();

        writer.handleStatement(vf.createStatement(dataset, VOID.PROPERTYPARTITION, prop));
        writer.handleStatement(vf.createStatement(prop, VOID.PROPERTY, predicate));

        writer.handleStatement(vf.createStatement(prop, VOID.TRIPLES, vf.createLiteral(triples)));
        writer.handleStatement(vf.createStatement(prop, VOID.DISTINCTSUBJECTS, vf.createLiteral(subjCount)));
        writer.handleStatement(vf.createStatement(prop, VOID.DISTINCTOBJECTS, vf.createLiteral(objCount)));

        for (String prefix: subjPrefix) {
            writer.handleStatement(vf.createStatement(prop, SEVOD.SUBJECTREGEXPATTERN, vf.createLiteral(prefix, XMLSchema.STRING)));
        }

        for (String prefix: objPrefix) {
            writer.handleStatement(vf.createStatement(prop, SEVOD.OBJECTREGEXPATTERN, vf.createLiteral(prefix, XMLSchema.STRING)));
        }
    }
}
