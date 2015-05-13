package eu.semagrow.stack.metadatagen;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
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
    private EndpointConnection conn;

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

    private void printPrefixes() {

/*        System.out.println("===========================");

        List<String> list1 = SubjectTrie.getPatterns();
        Collections.sort(list1);

        for (String s : list1) {
            System.out.println(s);
        }

        System.out.println("===========================");

        List<String> list2 = ObjectTrie.getPatterns();
        Collections.sort(list2);
        for (String s : list2) {
            System.out.println(s);
        }*/
    }


    private void writeSubjectStat(String subjectPattern) {

        int nTriples = 0, nDistObjects = 0;

        try {
            nTriples = conn.getSubjectNumberOfTriples(subjectPattern);
            nDistObjects = conn.getSubjectNumberOfDistinctObjects(subjectPattern);
        }
        catch (QueryEvaluationException e) {
            e.printStackTrace();
        }
        BNode propPartition = vf.createBNode();
        Literal count = vf.createLiteral(String.valueOf(nTriples));
        Literal distinctO = vf.createLiteral(String.valueOf(nDistObjects));
        try {
            writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.subset.toString()), propPartition));
            writer.handleStatement(vf.createStatement(propPartition, vf.createURI(SEVOD.subjectRegexPattern.toString()), vf.createURI(subjectPattern)));
            writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.triples.toString()), count));
            writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.distinctObjects.toString()), distinctO));
        } catch (RDFHandlerException e) {
            e.printStackTrace();
        }
    }

    private void writeObjectStat(String objectPattern) {

        int nTriples = 0, nDistSubjects = 0;

        try {
            nTriples = conn.getSubjectNumberOfTriples(objectPattern);
            nDistSubjects = conn.getObjectNumberOfDistinctSubjects(objectPattern);
        }
        catch (QueryEvaluationException e) {
            e.printStackTrace();
        }
        BNode propPartition = vf.createBNode();
        Literal count = vf.createLiteral(String.valueOf(nTriples));
        Literal distinctO = vf.createLiteral(String.valueOf(nDistSubjects));
        try {
            writer.handleStatement(vf.createStatement(dataset, vf.createURI(VOID.subset.toString()), propPartition));
            writer.handleStatement(vf.createStatement(propPartition, vf.createURI(SEVOD.subjectRegexPattern.toString()), vf.createURI(objectPattern)));
            writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.triples.toString()), count));
            writer.handleStatement(vf.createStatement(propPartition, vf.createURI(VOID.distinctObjects.toString()), distinctO));
        } catch (RDFHandlerException e) {
            e.printStackTrace();
        }
    }

    private void writeSevodStats() {

        try {
            conn = new EndpointConnection(endpoint);
        }
        catch (RepositoryException e) {
            e.printStackTrace();
        }

        /*List<String> list1 = SubjectTrie.getPatterns();
        Collections.sort(list1);

        for (String s : list1) {
            writeSubjectStat(s);
        }*/

        /*List<String> list2 = ObjectTrie.getPatterns();
        Collections.sort(list2);

        for (String s : list2) {
            writeObjectStat(s);
        }*/
    }

    // ------------------------------------------------------------------------

    public VoidGenerator(String e) {
        this.endpoint = e;
        SubjectTrie = new PathTrie(15);
        ObjectTrie = new PathTrie(350);
    }

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

        //for (String ns : namespaces.keySet())
        //    writer.handleNamespace(ns, namespaces.get(ns));

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

        //writeSevodStats();

        // TODO: write general statistics
        writeGeneralStats();

        writer.endRDF();

        printPrefixes();
    }

    // ------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // check for file parameter
        if (args.length < 2) {
            String className = VoidGenerator.class.getName();
            System.err.println("USAGE: java " + className + " endpoint RDF.nt{.zip}");
            System.exit(1);
        }

        File file = new File(args[1]);
        if (!file.exists()) {
            System.err.println("file not found: " + file);
            System.exit(1);
        }

        // check if file is not a directory
        if (!file.isFile()) {
            System.err.println("not a normal file: " + file);
            System.exit(1);
        }
        processFile(file, args[0]);

    }

    public static void processFile(File file, String endpoint) throws IOException {

        // check for gzip file
        if (file.getName().toLowerCase().contains(".gz")) {
            processInputStream(new GZIPInputStream(new FileInputStream(file)), file.getName(), endpoint);
        }

        // check for zip file
        else if (file.getName().toLowerCase().contains(".zip")) {
            ZipFile zf = new ZipFile(file);
            if (zf.size() > 1) {
                System.err.println("found multiple files in archive, processing only first one.");
            }
            ZipEntry entry = zf.entries().nextElement();
            if (entry.isDirectory()) {
                System.err.println("found directory instead of normal file in archive: " + entry.getName());
                System.exit(1);
            }

            processInputStream(zf.getInputStream(entry), entry.getName(), endpoint);
        }

        // process data stream of file
        else {
            processInputStream(new FileInputStream(file), file.getName(), endpoint);
        }
    }

    public static void processInputStream(InputStream input, String filename, String endpoint) throws IOException {

        long start = System.currentTimeMillis();
        System.err.println("processing " + filename);


        // identify parser format
        RDFFormat format = Rio.getParserFormatForFileName(filename);
        if (format == null) {
            System.err.println("can not identify RDF format for: " + filename);
            System.exit(1);
        }

        // initalize parser
        VoidGenerator handler = new VoidGenerator(endpoint);
        RDFParser parser = Rio.createParser(format);
//		parser.setVerifyData(false);
        parser.setStopAtFirstError(false);
        parser.setRDFHandler(handler);

        try {
            parser.parse(input, "");
        } catch (RDFParseException e) {
            System.err.println("encountered error while parsing " + filename + ": " + e.getMessage());
            System.exit(1);
        } catch (RDFHandlerException e) {
            System.err.println("encountered error while processing " + filename + ": " + e.getMessage());
            System.exit(1);
        } finally {
            input.close();
        }

        System.err.println((System.currentTimeMillis() - start) / 1000 + " seconds elapsed");
    }
}
