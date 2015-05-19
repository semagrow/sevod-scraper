package eu.semagrow.stack.metadatagen;

import eu.semagrow.stack.metadatagen.handler.ObjectHandler;
import eu.semagrow.stack.metadatagen.handler.SubjectHandler;
import eu.semagrow.stack.metadatagen.util.CompactBNodeTurtleWriter;
import eu.semagrow.stack.metadatagen.util.DistinctCounter;
import eu.semagrow.stack.metadatagen.vocabulary.VOID;
import eu.semagrow.stack.metadatagen.writer.ObjectWriter;
import eu.semagrow.stack.metadatagen.writer.SubjectWriter;
import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.*;
import org.openrdf.rio.helpers.BasicParserSettings;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Created by antonis on 14/5/2015.
 */
public class MetadataGenerator {

    final private Logger log = Logger.getLogger(MetadataGenerator.class);

    private ValueFactory vf = ValueFactoryImpl.getInstance();
    private BNode dataset = vf.createBNode();
    private RDFWriter writer = null;
    private RDFFormat format = RDFFormat.NQUADS;

    SubjectHandler subjecthandler = null;
    SubjectWriter subjectwriter = null;
    ObjectHandler objecthandler = null;
    ObjectWriter objectwriter = null;

    VoidGenerator voidGenerator = null;

    boolean generateSubjects = true;
    boolean generateObjects = true;

    int subjectBound = 15;
    int objectBound = 350;
    String endpoint = "http://localhost/";

    ///////////////////////////////////////////////////////////////////////////

    public MetadataGenerator(int sb, int ob, String e) {
        if (sb == -1)
            generateSubjects = false;
        if (ob == -1)
            generateObjects = false;
        if (sb > 0)
            subjectBound = sb;
        if (ob > 0)
            objectBound = ob;
        if (e != "")
            endpoint = e;
    }

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
        RDFParser parser = Rio.createParser(format);
        parser.getParserConfig().set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
        parser.setRDFHandler(voidGenerator);
        parser.parse(new FileInputStream(file), "");
    }

    ///////////////////////////////////////////////////////////////////////////

    public void writeMetadata(File infile, File outfile) throws RDFParseException, IOException, RDFHandlerException {

        writer = new CompactBNodeTurtleWriter(new FileOutputStream(outfile));

        writer.startRDF();

        writer.handleNamespace("void", "http://rdfs.org/ns/void#");
        writer.handleNamespace("svd", "http://rdf.iit.demokritos.gr/2013/sevod#");
        writer.handleNamespace("xsd", "http://www.w3.org/2001/XMLSchema#");

        writer.handleStatement(vf.createStatement(dataset, RDF.TYPE, vf.createURI(VOID.Dataset.toString())));

        if (generateSubjects)
            handleSubjects(infile);

        new DistinctCounter().clearAll();

        if (generateObjects)
            handleObjects(infile);

        new DistinctCounter().clearAll();

        handleProperties(infile);

        writer.endRDF();

        new DistinctCounter().clearAll();
    }
}
