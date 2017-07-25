package eu.semagrow.scraper.rdf;

import eu.semagrow.scraper.rdf.handler.ObjectHandler;
import eu.semagrow.scraper.rdf.handler.SubjectHandler;
import eu.semagrow.scraper.rdf.util.CompactBNodeTurtleWriter;
import eu.semagrow.scraper.rdf.vocabulary.VOID;
import eu.semagrow.scraper.rdf.writer.ObjectWriter;
import eu.semagrow.scraper.rdf.writer.SubjectWriter;
import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.*;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.turtle.TurtleWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by antonis on 14/5/2015.
 */
public class MetadataGenerator {

    final private Logger log = Logger.getLogger(MetadataGenerator.class);

    private ValueFactory vf = ValueFactoryImpl.getInstance();
    private BNode dataset = vf.createBNode();
    private RDFWriter writer = null;
    private RDFFormat format;

    SubjectHandler subjecthandler = null;
    SubjectWriter subjectwriter = null;
    ObjectHandler objecthandler = null;
    ObjectWriter objectwriter = null;

    VoidGenerator voidGenerator = null;

    Map<URI, Resource> propertyPartitionMap = null;

    boolean genSubjects = false;
    boolean genObjects = false;
    boolean genProperties = false;
    boolean genVocab = false;
    boolean genSelectivities = false;
    boolean use_endpoint = false;

    int subjectBound = 15;
    int objectBound = 350;
    String endpoint;

    ///////////////////////////////////////////////////////////////////////////

    public MetadataGenerator(String endpointStr) {
        endpoint = endpointStr;
    }

    public void setBounds(int sb, int ob) {
        if (sb > 0)
            subjectBound = sb;
        if (ob > 0)
            objectBound = ob;
    }

    public void setFormat(RDFFormat f) { format = f; }

    public void generateSubjects() { genSubjects = true; }

    public void generateObjects() { genObjects = true; }

    public void generateVocabulary() { genVocab = true; }

    public void generateProperties() { genProperties = true; }

    public void generateSelectivities() { genSelectivities = true; }

    public void useEndpoint() { use_endpoint = true; }

    private void handleSubjects(File file) throws RDFParseException, IOException, RDFHandlerException {

        log.debug("Discovering Subject Patterns...");
        subjecthandler = new SubjectHandler(subjectBound);
        RDFParser parser = Rio.createParser(format);
        parser.getParserConfig().set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
        parser.setRDFHandler(subjecthandler);
        parser.parse(new FileInputStream(file), "");
        List<String> patterns = subjecthandler.getPatterns();
        log.debug(patterns);
        log.debug("Found " + patterns.size() + " Subject Patterns.");

        log.debug("Generating Subject Metadata...");
        subjectwriter = new SubjectWriter(patterns);
        parser.setRDFHandler(subjectwriter);
        parser.parse(new FileInputStream(file), "");

        subjectwriter.writeSevodStats(writer, dataset);
    }

    private void handleObjects(File file) throws RDFParseException, IOException, RDFHandlerException {

        log.debug("Discovering Object Patterns...");
        objecthandler = new ObjectHandler(objectBound);
        RDFParser parser = Rio.createParser(format);
        parser.getParserConfig().set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
        parser.setRDFHandler(objecthandler);
        parser.parse(new FileInputStream(file), "");

        List<String> patterns = objecthandler.getPatterns();
        log.debug(patterns);
        log.debug("Found " + patterns.size() + " Object Patterns.");

        log.debug("Generating Object Metadata...");
        objectwriter = new ObjectWriter(patterns);
        parser.setRDFHandler(objectwriter);
        parser.parse(new FileInputStream(file), "");

        objectwriter.writeSevodStats(writer, dataset);
    }

    private void handleProperties(File file) throws RDFParseException, IOException, RDFHandlerException {
        log.debug("Generating VoID Metadata...");
        voidGenerator = new VoidGenerator(writer, dataset, endpoint);
        if (genVocab) {
            voidGenerator.generateVocabulary();
        }
        RDFParser parser = Rio.createParser(format);
        parser.getParserConfig().set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
        parser.setRDFHandler(voidGenerator);
        parser.parse(new FileInputStream(file), "");

        propertyPartitionMap = voidGenerator.getPropertiesMap();
    }

    private void handleSelectivities(File file, boolean use_endpoint) throws Exception {
        log.debug("Generating Selectivity Metadata...");
        SelectivityGenerator gen = new SelectivityGenerator(propertyPartitionMap);
        if (use_endpoint) {
            gen.calculateSelectivities(endpoint);
        }
        else {
            gen.calculateSelectivities(format, file);
        }
        gen.writeSelectivities(writer);
    }

    ///////////////////////////////////////////////////////////////////////////

    public void writeMetadata(File infile, File outfile) throws Exception {

        if (genSelectivities) {
            writer = new TurtleWriter(new FileOutputStream(outfile));
        }
        else {
            writer = new CompactBNodeTurtleWriter(new FileOutputStream(outfile));
        }

        writer.startRDF();

        writer.handleNamespace("void", "http://rdfs.org/ns/void#");
        writer.handleNamespace("svd", "http://rdf.iit.demokritos.gr/2013/sevod#");
        writer.handleNamespace("xsd", "http://www.w3.org/2001/XMLSchema#");

        writer.handleStatement(vf.createStatement(dataset, RDF.TYPE, vf.createURI(VOID.Dataset.toString())));

        if (genSubjects) {
            handleSubjects(infile);
        }

        if (genObjects) {
            handleObjects(infile);
        }

        if (genProperties) {
            handleProperties(infile);

            if (genSelectivities) {
                handleSelectivities(infile, use_endpoint);
            }
        }

        writer.endRDF();
    }
}
