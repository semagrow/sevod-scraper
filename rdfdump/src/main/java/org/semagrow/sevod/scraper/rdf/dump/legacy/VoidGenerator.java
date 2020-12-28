package org.semagrow.sevod.scraper.rdf.dump.legacy;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.helpers.RDFHandlerBase;
import org.semagrow.sevod.commons.vocabulary.SEVOD;
import org.semagrow.sevod.commons.vocabulary.VOID;
import org.semagrow.sevod.scraper.rdf.dump.util.FileDistinctCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * Created by antru on 21/4/2015.
 */
@Deprecated
public class VoidGenerator extends RDFHandlerBase {

    final private Logger log = LoggerFactory.getLogger(VoidGenerator.class);

    private final Map<IRI, Integer> typeCountMap = new HashMap<IRI, Integer>();
    private final Set<IRI> predicates = new HashSet<IRI>();

    private FileDistinctCounter distSubject;
    private FileDistinctCounter distObject;

    private final FileDistinctCounter distSubjectTotal = new FileDistinctCounter(null);
    private final FileDistinctCounter distObjectTotal = new FileDistinctCounter(null);

    private Map<IRI, Resource> propertyPartitionMap = new HashMap<>();

    private String endpoint;

    private IRI lastPredicate = null, curPredicate = null;
    private long predCount;
    private long tripleCount;
    private long entityCount;

    private boolean genVocab = false;

    private ValueFactory vf = SimpleValueFactory.getInstance();

    private Resource dataset;
    private final RDFWriter writer;

    private final Comparator<Value> VAL_COMP = new Comparator<Value>() {
        @Override
        public int compare(Value val1, Value val2) {
            return val1.stringValue().compareTo(val2.stringValue());
        }
    };

    // ------------------------------------------------------------------------

    public Map<IRI, Resource> getPropertiesMap() {
        return this.propertyPartitionMap;
    }

    private void countType(IRI type) {
        Integer count = typeCountMap.get(type);
        if (count == null) {
            typeCountMap.put(type, 1);
        } else {
            typeCountMap.put(type, 1 + count);
        }
    }

    /**
     * Stores types and predicates occurring with the current subject.
     *
     * @param st the Statement to process.
     */
    private void storeStatement(Statement st) {

        IRI predicate = st.getPredicate();
        predCount++;

        // check for type statement
        if (RDF.TYPE.equals(predicate)) {

            countType((IRI) st.getObject());
            entityCount++;
        }

        // store subject and object
        distSubject.add(st.getSubject());
        distObject.add(st.getObject());

        distSubjectTotal.add(st.getSubject());
        distObjectTotal.add(st.getObject());

        lastPredicate = predicate;
        curPredicate = st.getPredicate();
    }

    /**
     * Analyzes the last statements (which have the same subject)
     * and counts the predicates per type.
     */
    private void processStoredStatements() {
        if (lastPredicate == null)
            return;

        predicates.add(lastPredicate);

        writePredicateStatToVoid(lastPredicate, predCount, distSubject.getDistinctCount(), distObject.getDistinctCount());

        // clear stored values;
        distSubject.close();
        distObject.close();
        distSubject = new FileDistinctCounter(null);
        distObject = new FileDistinctCounter(null);

        predCount = 0;
    }

    private void writePredicateStatToVoid(IRI predicate, long pCount, int distS, int distO) {
        log.debug("Writing VoID statistics of predicate " + predicate.toString());
        BNode propPartition = vf.createBNode();
        Literal count = vf.createLiteral(pCount);
        Literal distinctS = vf.createLiteral(distS);
        Literal distinctO = vf.createLiteral(distO);
        try {
            writer.handleStatement(vf.createStatement(dataset, VOID.PROPERTYPARTITION, propPartition));
            writer.handleStatement(vf.createStatement(propPartition, VOID.PROPERTY, predicate));
            writer.handleStatement(vf.createStatement(propPartition, VOID.TRIPLES, count));
            writer.handleStatement(vf.createStatement(propPartition, VOID.DISTINCTSUBJECTS, distinctS));
            writer.handleStatement(vf.createStatement(propPartition, VOID.DISTINCTOBJECTS, distinctO));
            if (genVocab) {
                writeSummaries(propPartition, predicate);
            }
            propertyPartitionMap.put(predicate, propPartition);
        } catch (RDFHandlerException e) {
            e.printStackTrace();
        }
    }

    private void writeSummaries(BNode propPartition, IRI predicate) {
        log.debug("Writing SEVOD vocabularies of predicate " + predicate.toString());
        for (String voc : distSubject.getAuthorities()) {
            try {
                if (!voc.isEmpty()) {
                    writer.handleStatement(vf.createStatement(propPartition, SEVOD.SUBJECTVOCABULARY, vf.createIRI(voc)));
                }
            } catch (RDFHandlerException e) {
                e.printStackTrace();
            }
        }
        for (String voc : distObject.getAuthorities()) {
            try {
                if (!voc.isEmpty()) {
                    writer.handleStatement(vf.createStatement(propPartition, SEVOD.OBJECTVOCABULARY, vf.createIRI(voc)));
                }
            } catch (RDFHandlerException e) {
                e.printStackTrace();
            }
        }
    }

    private void writeTypeStatToVoid(Value type, long tCount) {
        log.debug("Writing VoID statistics of type " + type.toString());
        BNode classPartition = vf.createBNode();
        Literal count = vf.createLiteral(tCount);
        try {
            writer.handleStatement(vf.createStatement(dataset, VOID.CLASSPARTITION, classPartition));
            writer.handleStatement(vf.createStatement(classPartition, VOID.CLASS, type));
            writer.handleStatement(vf.createStatement(classPartition, VOID.ENTITIES, count));
        } catch (RDFHandlerException e) {
            e.printStackTrace();
        }
    }

    private void writeGeneralStats() {
        log.debug("Writing general VoID statistics");
        try {
            writer.handleStatement(vf.createStatement(dataset, VOID.SPARQLENDPOINT, vf.createIRI(endpoint)));
            writer.handleStatement(vf.createStatement(dataset, VOID.TRIPLES, vf.createLiteral(tripleCount)));
            writer.handleStatement(vf.createStatement(dataset, VOID.PROPERTIES, vf.createLiteral(predicates.size())));
            writer.handleStatement(vf.createStatement(dataset, VOID.CLASSES, vf.createLiteral(typeCountMap.size())));
            writer.handleStatement(vf.createStatement(dataset, VOID.ENTITIES, vf.createLiteral(entityCount)));
            writer.handleStatement(vf.createStatement(dataset, VOID.DISTINCTSUBJECTS, vf.createLiteral(distSubjectTotal.getDistinctCount())));
            writer.handleStatement(vf.createStatement(dataset, VOID.DISTINCTOBJECTS, vf.createLiteral(distObjectTotal.getDistinctCount())));
        } catch (RDFHandlerException e) {
            e.printStackTrace();
        }
    }

    // ------------------------------------------------------------------------

    public VoidGenerator(RDFWriter w, Resource d, String e) {
        writer = w;
        dataset = d;
        endpoint = e;
    }

    public void generateVocabulary() {
        genVocab = true;
    }


    @Override
    public void startRDF() throws RDFHandlerException {
        super.startRDF();
        distSubject = new FileDistinctCounter(null);
        distObject = new FileDistinctCounter(null);
    }


    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        log.debug("Handling statement " + st.toString());

        tripleCount++;

        curPredicate = st.getPredicate();

        // check if current triple has different predicate than the last triple
        if (!st.getPredicate().equals(lastPredicate)) {
            processStoredStatements();
        }

        storeStatement(st);
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        super.endRDF();

        processStoredStatements();

        distObject.close();
        distSubject.close();

        // write type statistics
        List<IRI> types = new ArrayList<IRI>(typeCountMap.keySet());
        Collections.sort(types, VAL_COMP);
        for (IRI uri : types) {
            writeTypeStatToVoid(uri, typeCountMap.get(uri));
        }

        // TODO: write general statistics
        writeGeneralStats();
    }

}
