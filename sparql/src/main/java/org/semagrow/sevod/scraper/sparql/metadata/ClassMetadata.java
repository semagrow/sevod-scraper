package org.semagrow.sevod.scraper.sparql.metadata;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
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

public class ClassMetadata implements Metadata {

    private ValueFactory vf = SimpleValueFactory.getInstance();

    private IRI clazz;
    private String graph;
    private Set<String> knownPrefixes;

    private int entityCount = 0;
    private Set<String> entityPrefix = new HashSet<>();

    public ClassMetadata(IRI clazz, String graph, Set<String> knownPrefixes) {
        this.clazz = clazz;
        this.graph = graph;
        this.knownPrefixes = knownPrefixes;
    }

    @Override
    public void processEndpoint(String endpoint) {

        QueryEvaluator eval = new QueryEvaluator(endpoint);
        QueryTransformer qt = new QueryTransformer();

        entityCount = eval.count(qt.from(Queries.entities_count).setClass(clazz).setGraph(graph).toString());

        for (String prefix: knownPrefixes) {
            if (eval.ask(qt.from(Queries.entity_prefix).setClass(clazz).setGraph(graph).setPrefix(prefix).toString())) {
                entityPrefix.add(prefix);
            }
        }
    }

    @Override
    public void serializeMetadata(Resource dataset, RDFWriter writer) throws RDFHandlerException {
        BNode clzp = vf.createBNode();

        writer.handleStatement(vf.createStatement(dataset, VOID.CLASSPARTITION, clzp));
        writer.handleStatement(vf.createStatement(clzp, VOID.CLASS, clazz));
        writer.handleStatement(vf.createStatement(clzp, VOID.ENTITIES, vf.createLiteral(entityCount)));

        for (String prefix: entityPrefix) {
            writer.handleStatement(vf.createStatement(clzp, SEVOD.SUBJECTREGEXPATTERN, vf.createLiteral(prefix, XMLSchema.STRING)));
        }
    }
}
