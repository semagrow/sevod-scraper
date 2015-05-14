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

    private PathTrie SubjectTrie;
    private PathTrie ObjectTrie;

    private URI lastPredicate = null;
    private long predCount;
    private long tripleCount;
    private long entityCount;

    private String endpoint;
    private int count = 0;

    private ValueFactory vf = ValueFactoryImpl.getInstance();
    private BNode dataset = vf.createBNode();

    private final RDFWriter writer = new CompactBNodeTurtleWriter(System.out);

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

        if (st.getSubject() instanceof  URI) {
            String str = ((URI) st.getSubject()).toString();
            SubjectTrie.addPath(str);
	    }

        if (st.getObject() instanceof  URI) {
            String str = ((URI) st.getObject()).toString();
            ObjectTrie.addPath(str);
        }
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
//		System.out.println(lastPredicate + " [" + predCount + "], distS: " + distSubject.size() + ", distObj: " + distObject.size());
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

    /*
    public VoidGenerator(String e) {
        this.endpoint = e;
        SubjectTrie = new PathTrie(15);
        ObjectTrie = new PathTrie(350);
    }
    */

    @Override
    public void startRDF() throws RDFHandlerException {

        /*Map<String, String> namespaces = new HashMap<String, String>();

        namespaces.put("xsd", "http://www.w3.org/2001/XMLSchema#");
        namespaces.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        namespaces.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
        namespaces.put("dc", "http://purl.org/dc/elements/1.1/");
        namespaces.put("bio", "http://bio2rdf.org/ns/bio2rdf#");
        namespaces.put("chebi", "http://bio2rdf.org/ns/chebi#");*/

        super.startRDF();

        writer.startRDF();

        // following namespaces which will be shortened automatically
        writer.handleNamespace("void", "http://rdfs.org/ns/void#");
        writer.handleNamespace("svd", "http://rdf.iit.demokritos.gr/2013/sevod#");

        /*for (String ns : namespaces.keySet())
            writer.handleNamespace(ns, namespaces.get(ns));*/

        // general void information
        writer.handleStatement(vf.createStatement(dataset, RDF.TYPE, vf.createURI(VOID.Dataset.toString())));

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

        writer.endRDF();
    }

}
