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
import org.semagrow.sevod.scraper.rdf.dump.api.DistinctCounter;
import org.semagrow.sevod.scraper.rdf.dump.util.MapDBDistinctCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RdfDumpMetadataExtractor extends RDFHandlerBase {

    final private Logger log = LoggerFactory.getLogger(RdfDumpMetadataExtractor.class);

    private String endpoint;
    private Set<String> knownPrefixes;
    private RDFWriter writer;

    private Map<URI,PredicateMetadata> predicates;
    private Map<URI,ClassMetadata> classes;
    private DatasetMetadata datasetMetadata;

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

        datasetMetadata.serializeMetadata(dataset, writer);

        super.endRDF();
    }

    private class PredicateMetadata {

        private URI predicate;
        private Set<String> knownPrefixes;

        private int triples = 0;
        private DistinctCounter subjCount = new MapDBDistinctCounter();
        private DistinctCounter objCount = new MapDBDistinctCounter();
        private Set<String> subjPrefix = new HashSet<>();
        private Set<String> objPrefix = new HashSet<>();

        public PredicateMetadata(URI predicate, Set<String> knownPrefixes) {
            this.predicate = predicate;
            this.knownPrefixes = knownPrefixes;
        }

        public void processStatement(Statement statement) {
            assert statement.getPredicate().equals(predicate);

            Value s = statement.getSubject();
            Value o = statement.getObject();

            subjCount.add(s);
            objCount.add(o);

            for (String prefix: knownPrefixes) {
                if (s.stringValue().startsWith(prefix)) {
                    subjPrefix.add(prefix);
                }
                if (o.stringValue().startsWith(prefix)) {
                    objPrefix.add(prefix);
                }
            }

            triples++;
        }

        public void serializeMetadata(Resource dataset, RDFWriter writer) throws RDFHandlerException {
            BNode prop = vf.createBNode();

            writer.handleStatement(vf.createStatement(dataset, VOID.PROPERTYPARTITION, prop));
            writer.handleStatement(vf.createStatement(prop, VOID.PROPERTY, predicate));

            writer.handleStatement(vf.createStatement(prop, VOID.TRIPLES, vf.createLiteral(triples)));
            writer.handleStatement(vf.createStatement(prop, VOID.DISTINCTSUBJECTS, vf.createLiteral(subjCount.getDistinctCount())));
            writer.handleStatement(vf.createStatement(prop, VOID.DISTINCTOBJECTS, vf.createLiteral(objCount.getDistinctCount())));

            for (String prefix: subjPrefix) {
                writer.handleStatement(vf.createStatement(prop, SEVOD.SUBJECTREGEXPATTERN, vf.createLiteral(prefix, XMLSchema.STRING)));
            }

            for (String prefix: objPrefix) {
                writer.handleStatement(vf.createStatement(prop, SEVOD.OBJECTREGEXPATTERN, vf.createLiteral(prefix, XMLSchema.STRING)));
            }
        }
    }

    private class ClassMetadata {

        private URI clazz;
        private Set<String> knownPrefixes;

        private DistinctCounter entityCount = new MapDBDistinctCounter();
        private Set<String> entityPrefix = new HashSet<>();

        public ClassMetadata(URI clazz, Set<String> knownPrefixes) {
            this.clazz = clazz;
            this.knownPrefixes = knownPrefixes;
        }

        public void processStatement(Statement statement) {
            assert statement.getPredicate().equals(RDF.TYPE);
            assert statement.getObject().equals(clazz);

            Value s = statement.getSubject();

            entityCount.add(s);

            for (String prefix: knownPrefixes) {
                if (s.stringValue().startsWith(prefix)) {
                    entityPrefix.add(prefix);
                }
            }
        }

        public void serializeMetadata(Resource dataset, RDFWriter writer) throws RDFHandlerException {
            BNode clzp = vf.createBNode();

            writer.handleStatement(vf.createStatement(dataset, VOID.CLASSPARTITION, clzp));
            writer.handleStatement(vf.createStatement(clzp, VOID.CLASS, clazz));
            writer.handleStatement(vf.createStatement(clzp, VOID.ENTITIES, vf.createLiteral(entityCount.getDistinctCount())));

            for (String prefix: entityPrefix) {
                writer.handleStatement(vf.createStatement(clzp, SEVOD.SUBJECTREGEXPATTERN, vf.createLiteral(prefix, XMLSchema.STRING)));
            }
        }
    }

    private class DatasetMetadata {

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

        public void serializeMetadata(Resource dataset, RDFWriter writer) throws RDFHandlerException {
            writer.handleStatement(vf.createStatement(dataset, VOID.SPARQLENDPOINT, vf.createURI(endpoint)));
            writer.handleStatement(vf.createStatement(dataset, VOID.TRIPLES, vf.createLiteral(triples)));
            writer.handleStatement(vf.createStatement(dataset, VOID.PROPERTIES, vf.createLiteral(propCount.getDistinctCount())));
            writer.handleStatement(vf.createStatement(dataset, VOID.CLASSES, vf.createLiteral(clzCount.getDistinctCount())));
            writer.handleStatement(vf.createStatement(dataset, VOID.ENTITIES, vf.createLiteral(entCount.getDistinctCount())));
            writer.handleStatement(vf.createStatement(dataset, VOID.DISTINCTSUBJECTS, vf.createLiteral(subjCount.getDistinctCount())));
            writer.handleStatement(vf.createStatement(dataset, VOID.DISTINCTOBJECTS, vf.createLiteral(objCount.getDistinctCount())));
        }
    }
}
