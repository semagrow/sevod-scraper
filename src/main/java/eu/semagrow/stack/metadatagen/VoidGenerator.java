package eu.semagrow.stack.metadatagen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.semagrow.stack.metadatagen.extractor.PathTrie;
import eu.semagrow.stack.metadatagen.util.CompactBNodeTurtleWriter;
import eu.semagrow.stack.metadatagen.util.DistinctCounter;
import eu.semagrow.stack.metadatagen.vocabulary.SEVOD;
import eu.semagrow.stack.metadatagen.vocabulary.VOID;
import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.helpers.RDFHandlerBase;

import static java.lang.Math.max;

/**
 * Created by antru on 21/4/2015.
 */

public class VoidGenerator extends RDFHandlerBase {

    final private Logger log = Logger.getLogger(VoidGenerator.class);

    private final Map<URI, Integer> typeCountMap = new HashMap<URI, Integer>();
    private final Set<URI> predicates = new HashSet<URI>();

    private DistinctCounter distSubject;
    private DistinctCounter distObject;

    private final DistinctCounter distSubjectTotal = new DistinctCounter(null);
    private final DistinctCounter distObjectTotal = new DistinctCounter(null);

    private String endpoint;

    private URI lastPredicate = null, curPredicate = null;
    private long predCount;
    private long tripleCount;
    private long entityCount;

    private boolean genVocab = false;

    private ValueFactory vf = ValueFactoryImpl.getInstance();

    private Resource dataset;
    private final RDFWriter writer;

    private final Comparator<Value> VAL_COMP = new Comparator<Value>() {
        @Override
        public int compare(Value val1, Value val2) {
            return val1.stringValue().compareTo(val2.stringValue());
        }
    };

    // ------------------------------------------------------------------------

    private void countType(URI type) {
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

        URI predicate = st.getPredicate();
        predCount++;

        // check for type statement
        if (RDF.TYPE.equals(predicate)) {

            countType((URI) st.getObject());
            entityCount++;
        }

        // store subject and object
        distSubject.add(st.getSubject().toString());
        distObject.add(st.getObject().toString());

        distSubjectTotal.add(st.getSubject().toString());
        distObjectTotal.add(st.getObject().toString());

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
        distSubject = new DistinctCounter(null);
        distObject = new DistinctCounter(null);

        predCount = 0;
    }

    private void writePredicateStatToVoid(URI predicate, long pCount, int distS, int distO) {
        log.debug("Writing VoID statistics of predicate " + predicate.toString());
        BNode propPartition = vf.createBNode();
        Literal count = vf.createLiteral(pCount);
        Literal distinctS = vf.createLiteral(distS);
        Literal distinctO = vf.createLiteral(distO);
        try {
            writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.propertyPartition.toString()), propPartition));
            writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.property.toString()), predicate));
            writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.triples.toString()), count));
            writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.distinctSubjects.toString()), distinctS));
            writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.distinctObjects.toString()), distinctO));
            if (genVocab) {
                writeSummaries(propPartition, predicate);
            }
        } catch (RDFHandlerException e) {
            e.printStackTrace();
        }
    }

    private void writeSummaries(BNode propPartition, URI predicate) {
        log.debug("Writing SEVOD vocabularies of predicate " + predicate.toString());
        for (String voc : distSubject.getAuthorities()) {
            try {
                if (!voc.isEmpty()) {
                    writer.handleStatement(vf.createStatement(propPartition, vf.createURI(SEVOD.subjectVocabulary.toString()), vf.createURI(voc)));
                }
            } catch (RDFHandlerException e) {
                e.printStackTrace();
            }
        }
        for (String voc : distObject.getAuthorities()) {
            try {
                if (!voc.isEmpty()) {
                    writer.handleStatement(vf.createStatement(propPartition, vf.createURI(SEVOD.objectVocabulary.toString()), vf.createURI(voc)));
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
            writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.classPartition.toString()), classPartition));
            writer.handleStatement(vf.createStatement(classPartition, vf.createURI(VOID.clazz.toString()), type));
            writer.handleStatement(vf.createStatement(classPartition, vf.createURI(VOID.entities.toString()), count));
        } catch (RDFHandlerException e) {
            e.printStackTrace();
        }
    }

    private void writeGeneralStats() {
        log.debug("Writing general VoID statistics");
        try {
            writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.sparqlEndpoint.toString()), vf.createURI(endpoint)));
            writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.triples.toString()), vf.createLiteral(tripleCount)));
            writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.properties.toString()), vf.createLiteral(predicates.size())));
            writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.classes.toString()), vf.createLiteral(typeCountMap.size())));
            writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.entities.toString()), vf.createLiteral(entityCount)));
            writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.distinctSubjects.toString()), vf.createLiteral(distSubjectTotal.getDistinctCount())));
            writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.distinctObjects.toString()), vf.createLiteral(distObjectTotal.getDistinctCount())));
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
        distSubject = new DistinctCounter(null);
        distObject = new DistinctCounter(null);
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
        List<URI> types = new ArrayList<URI>(typeCountMap.keySet());
        Collections.sort(types, VAL_COMP);
        for (URI uri : types) {
            writeTypeStatToVoid(uri, typeCountMap.get(uri));
        }

        // TODO: write general statistics
        writeGeneralStats();
    }

}
