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
import eu.semagrow.stack.metadatagen.vocabulary.VOID;
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

    private final Map<URI, Integer> typeCountMap = new HashMap<URI, Integer>();
    private final Set<URI> predicates = new HashSet<URI>();
    private final Set<Resource> distSubject = new HashSet<Resource>();
    private final Set<Value> distObject = new HashSet<Value>();

    private URI lastPredicate = null;
    private long predCount;
    private long tripleCount;
    private long entityCount;

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
        distSubject.add(st.getSubject());
        distObject.add(st.getObject());

        lastPredicate = predicate;
    }

    /**
     * Analyzes the last statements (which have the same subject)
     * and counts the predicates per type.
     */
    private void processStoredStatements() {
        if (lastPredicate == null)
            return;

        predicates.add(lastPredicate);

        // TODO: write predicate statistics
        //System.out.println(lastPredicate + " [" + predCount + "], distS: " + distSubject.size() + ", distObj: " + distObject.size());
        writePredicateStatToVoid(lastPredicate, predCount, distSubject.size(), distObject.size());

        // clear stored values;
        distSubject.clear();
        distObject.clear();
        predCount = 0;
    }

    private void writePredicateStatToVoid(URI predicate, long pCount, int distS, int distO) {
        BNode propPartition = vf.createBNode();
        Literal count = vf.createLiteral(String.valueOf(pCount));
        Literal distinctS = vf.createLiteral(String.valueOf(distS));
        Literal distinctO = vf.createLiteral(String.valueOf(distO));
        try {
            writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.propertyPartition.toString()), propPartition));
            writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.property.toString()), predicate));
            writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.triples.toString()), count));
            writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.distinctSubjects.toString()), distinctS));
            writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.distinctObjects.toString()), distinctO));
        } catch (RDFHandlerException e) {
            e.printStackTrace();
        }
    }

    private void writeTypeStatToVoid(Value type, long tCount) {
        BNode classPartition = vf.createBNode();
        Literal count = vf.createLiteral(String.valueOf(tCount));
        try {
            writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.classPartition.toString()), classPartition));
            writer.handleStatement(vf.createStatement(classPartition, vf.createURI(VOID.clazz.toString()), type));
            writer.handleStatement(vf.createStatement(classPartition, vf.createURI(VOID.entities.toString()), count));
        } catch (RDFHandlerException e) {
            e.printStackTrace();
        }
    }

    private void writeGeneralStats() {

        try {
            writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.triples.toString()), vf.createLiteral(String.valueOf(tripleCount))));
            writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.properties.toString()), vf.createLiteral(String.valueOf(predicates.size()))));
            writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.classes.toString()), vf.createLiteral(String.valueOf(typeCountMap.size()))));
            writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.entities.toString()), vf.createLiteral(String.valueOf(entityCount))));
        } catch (RDFHandlerException e) {
            e.printStackTrace();
        }
    }

    // ------------------------------------------------------------------------

    public VoidGenerator(RDFWriter w, Resource d) {
        this.writer = w;
        this.dataset = d;
        //ObjectTrie = new PathTrie(350);
    }


    @Override
    public void startRDF() throws RDFHandlerException {
        super.startRDF();
    }


    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {

        tripleCount++;

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
