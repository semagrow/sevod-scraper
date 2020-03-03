package org.semagrow.sevod.scraper.rdf.dump;

import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.helpers.RDFHandlerBase;
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
    private RDFWriter writer;

    private Map<URI,Metadata> predicates;
    private Map<URI,Metadata> classes;
    private Metadata datasetMetadata;
    private Metadata boundaryMetadata;

    private ValueFactory vf = ValueFactoryImpl.getInstance();

    public RdfDumpMetadataExtractor(String endpoint, Set<String> knownPrefixes, RDFWriter writer) {
        this.endpoint = endpoint;
        this.knownPrefixes = knownPrefixes;
        this.writer = writer;
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        super.startRDF();

        predicates = new HashMap<>();
        classes = new HashMap<>();
        datasetMetadata = new DatasetMetadata(endpoint);
        boundaryMetadata = new BoundaryMetadata();

        writer.handleNamespace(VOID.PREFIX, VOID.NAMESPACE);
        writer.handleNamespace(SEVOD.PREFIX, SEVOD.NAMESPACE);
        writer.handleNamespace(XMLSchema.PREFIX, XMLSchema.NAMESPACE);
    }

    @Override
    public void handleStatement(Statement st) {
        log.info("Handling statement " + st.toString());

        URI p = st.getPredicate();

        if (!predicates.containsKey(p)) {
            predicates.put(p, new PredicateMetadata(p, knownPrefixes));
        }
        predicates.get(p).processStatement(st);

        if (p.equals(RDF.TYPE)) {
            URI c = (URI) st.getObject();
            if (!classes.containsKey(c)) {
                classes.put(c, new ClassMetadata(c, knownPrefixes));
            }
            classes.get(c).processStatement(st);
        }

        datasetMetadata.processStatement(st);
        boundaryMetadata.processStatement(st);
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        Resource dataset = vf.createBNode();

        writer.handleStatement(vf.createStatement(dataset, RDF.TYPE, VOID.DATASET));

        for (URI p: predicates.keySet()) {
            log.info("Writing metadata of predicate " + p.stringValue());
            predicates.get(p).serializeMetadata(dataset, writer);
        }

        for (URI c: classes.keySet()) {
            log.info("Writing metadata of class " + c.stringValue());
            classes.get(c).serializeMetadata(dataset, writer);
        }

        log.info("Writing general metadata");
        datasetMetadata.serializeMetadata(dataset, writer);
        boundaryMetadata.serializeMetadata(dataset, writer);

        super.endRDF();
    }
}
