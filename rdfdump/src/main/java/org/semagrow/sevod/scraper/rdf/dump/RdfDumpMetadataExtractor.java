package org.semagrow.sevod.scraper.rdf.dump;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.helpers.RDFHandlerBase;
import org.semagrow.sevod.commons.vocabulary.SEVOD;
import org.semagrow.sevod.commons.vocabulary.VOID;
import org.semagrow.sevod.scraper.rdf.dump.metadata.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RdfDumpMetadataExtractor extends RDFHandlerBase {

    final private Logger log = LoggerFactory.getLogger(RdfDumpMetadataExtractor.class);

    private String endpoint;
    private Set<String> knownPrefixes;
    protected RDFWriter writer;

    private Map<IRI,Metadata> predicates;
    private Map<IRI,Metadata> classes;
    private Metadata datasetMetadata;

    private ValueFactory vf = SimpleValueFactory.getInstance();

    protected Resource dataset = vf.createBNode();

    public RdfDumpMetadataExtractor(String endpoint, Set<String> knownPrefixes, RDFWriter writer) {
        this.endpoint = endpoint;
        this.knownPrefixes = knownPrefixes;
        this.writer = writer;
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        super.startRDF();

        log.info("Scraping the dataset...");

        predicates = new HashMap<>();
        classes = new HashMap<>();
        datasetMetadata = new DatasetMetadata(endpoint);

        writer.handleNamespace(VOID.PREFIX, VOID.NAMESPACE);
        writer.handleNamespace(SEVOD.PREFIX, SEVOD.NAMESPACE);
        writer.handleNamespace(XMLSchema.PREFIX, XMLSchema.NAMESPACE);
    }

    @Override
    public void handleStatement(Statement st) {

        IRI p = st.getPredicate();

        if (!predicates.containsKey(p)) {
            predicates.put(p, new PredicateMetadata(p, knownPrefixes));
        }
        predicates.get(p).processStatement(st);

        if (p.equals(RDF.TYPE)) {
            IRI c = (IRI) st.getObject();
            if (!classes.containsKey(c)) {
                classes.put(c, new ClassMetadata(c, knownPrefixes));
            }
            classes.get(c).processStatement(st);
        }

        datasetMetadata.processStatement(st);
    }

    @Override
    public void endRDF() throws RDFHandlerException {

        log.info("Writing dataset metadata...");

        writer.handleStatement(vf.createStatement(dataset, RDF.TYPE, VOID.DATASET));

        for (IRI p: predicates.keySet()) {
            predicates.get(p).serializeMetadata(dataset, writer);
        }

        for (IRI c: classes.keySet()) {
            classes.get(c).serializeMetadata(dataset, writer);
        }

        datasetMetadata.serializeMetadata(dataset, writer);

        super.endRDF();
    }
}
