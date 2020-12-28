package org.semagrow.sevod.scraper.sparql.metadata;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.semagrow.sevod.commons.vocabulary.VOID;
import org.semagrow.sevod.scraper.sparql.Queries;
import org.semagrow.sevod.scraper.sparql.QueryEvaluator;
import org.semagrow.sevod.scraper.sparql.QueryTransformer;

public class DatasetMetadata implements Metadata {

    private ValueFactory vf = SimpleValueFactory.getInstance();

    private String graph;

    private String endpoint = "http://endpoint";

    private int triples = 0;
    private int propCount = 0;
    private int clzCount = 0;
    private int entCount = 0;
    private int subjCount = 0;
    private int objCount = 0;

    public DatasetMetadata(String graph) {
        this.graph = graph;
    }

    @Override
    public void processEndpoint(String endpoint) {
        this.endpoint = endpoint;

        QueryEvaluator eval = new QueryEvaluator(endpoint);
        QueryTransformer qt = new QueryTransformer();

        triples = eval.count(qt.from(Queries.triples_count).setGraph(graph).toString());
        propCount = eval.count(qt.from(Queries.predicates_count).setGraph(graph).toString());
        clzCount = eval.count(qt.from(Queries.classes_count).setGraph(graph).toString());
        entCount = eval.count(qt.from(Queries.entities_count).setGraph(graph).toString());
        subjCount = eval.count(qt.from(Queries.subjects_count).setGraph(graph).toString());
        objCount = eval.count(qt.from(Queries.objects_count).setGraph(graph).toString());
    }

    @Override
    public void serializeMetadata(Resource dataset, RDFWriter writer) throws RDFHandlerException {
        writer.handleStatement(vf.createStatement(dataset, VOID.SPARQLENDPOINT, vf.createIRI(endpoint)));
        writer.handleStatement(vf.createStatement(dataset, VOID.TRIPLES, vf.createLiteral(triples)));
        writer.handleStatement(vf.createStatement(dataset, VOID.PROPERTIES, vf.createLiteral(propCount)));
        writer.handleStatement(vf.createStatement(dataset, VOID.CLASSES, vf.createLiteral(clzCount)));
        writer.handleStatement(vf.createStatement(dataset, VOID.ENTITIES, vf.createLiteral(entCount)));
        writer.handleStatement(vf.createStatement(dataset, VOID.DISTINCTSUBJECTS, vf.createLiteral(subjCount)));
        writer.handleStatement(vf.createStatement(dataset, VOID.DISTINCTOBJECTS, vf.createLiteral(objCount)));
    }
}