package org.semagrow.sevod.scraper.rdf.dump.metadata;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.semagrow.sevod.commons.vocabulary.VOID;
import org.semagrow.sevod.scraper.rdf.dump.util.DistinctCounter;
import org.semagrow.sevod.scraper.rdf.dump.util.MapDBDistinctCounter;

public class DatasetMetadata implements Metadata {

    private ValueFactory vf = SimpleValueFactory.getInstance();

    private String endpoint;

    private int triples = 0;
    private DistinctCounter propCount = new MapDBDistinctCounter();
    private DistinctCounter clzCount = new MapDBDistinctCounter();
    private DistinctCounter entCount = new MapDBDistinctCounter();
    private DistinctCounter subjCount = new MapDBDistinctCounter();
    private DistinctCounter objCount = new MapDBDistinctCounter();

    public DatasetMetadata(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public void processStatement(Statement statement) {

        Value s = statement.getSubject();
        Value p = statement.getPredicate();
        Value o = statement.getObject();

        subjCount.add(s);
        propCount.add(p);
        objCount.add(o);

        if (p.equals(RDF.TYPE)) {
            entCount.add(s);
            clzCount.add(o);
        }

        triples++;
    }

    @Override
    public void serializeMetadata(Resource dataset, RDFWriter writer) throws RDFHandlerException {
        writer.handleStatement(vf.createStatement(dataset, VOID.SPARQLENDPOINT, vf.createIRI(endpoint)));
        writer.handleStatement(vf.createStatement(dataset, VOID.TRIPLES, vf.createLiteral(triples)));
        writer.handleStatement(vf.createStatement(dataset, VOID.PROPERTIES, vf.createLiteral(propCount.getDistinctCount())));
        writer.handleStatement(vf.createStatement(dataset, VOID.CLASSES, vf.createLiteral(clzCount.getDistinctCount())));
        writer.handleStatement(vf.createStatement(dataset, VOID.ENTITIES, vf.createLiteral(entCount.getDistinctCount())));
        writer.handleStatement(vf.createStatement(dataset, VOID.DISTINCTSUBJECTS, vf.createLiteral(subjCount.getDistinctCount())));
        writer.handleStatement(vf.createStatement(dataset, VOID.DISTINCTOBJECTS, vf.createLiteral(objCount.getDistinctCount())));
    }
}