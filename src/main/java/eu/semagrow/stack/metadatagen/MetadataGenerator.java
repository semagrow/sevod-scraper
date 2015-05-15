package eu.semagrow.stack.metadatagen;

import eu.semagrow.stack.metadatagen.util.CompactBNodeTurtleWriter;
import eu.semagrow.stack.metadatagen.vocabulary.VOID;
import org.openrdf.model.BNode;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

/**
 * Created by antonis on 14/5/2015.
 */
public class MetadataGenerator {

    private ValueFactory vf = ValueFactoryImpl.getInstance();
    private BNode dataset = vf.createBNode();
    private RDFWriter writer = new CompactBNodeTurtleWriter(System.out);
    private RDFFormat format = RDFFormat.NQUADS;

    SubjectHandler subjecthandler = null;
    SubjectWriter subjectwriter = null;
    VoidGenerator voidGenerator = null;

    SubjectHandler getSubjecthandler() { return subjecthandler; }

    SubjectWriter getSubjectwriter() { return subjectwriter; }

    VoidGenerator getVoidGenerator() { return voidGenerator; }

    ///////////////////////////////////////////////////////////////////////////

    private void handleSubjects(File file) throws RDFParseException, IOException, RDFHandlerException {

        subjecthandler = new SubjectHandler();
        RDFParser parser = Rio.createParser(format);
        parser.setRDFHandler(subjecthandler);
        parser.parse(new FileInputStream(file), "");

        List<String> list = subjecthandler.getPatterns();

        subjectwriter = new SubjectWriter(list);
        parser.setRDFHandler(subjectwriter);
        parser.parse(new FileInputStream(file), "");

        subjectwriter.writeSevodStats(writer, dataset);
    }

    private void handleObjects(File file) throws RDFParseException, IOException, RDFHandlerException {
        // TODO
    }

    private void handleProperties(File file) throws RDFParseException, IOException, RDFHandlerException {
        voidGenerator = new VoidGenerator(writer, dataset);
        RDFParser parser = Rio.createParser(format);
        parser.setRDFHandler(voidGenerator);
        parser.parse(new FileInputStream(file), "");
    }

    ///////////////////////////////////////////////////////////////////////////

    public void writeMetadata(File file) throws RDFParseException, IOException, RDFHandlerException {

        writer.startRDF();

        writer.handleNamespace("void", "http://rdfs.org/ns/void#");
        writer.handleNamespace("svd", "http://rdf.iit.demokritos.gr/2013/sevod#");

        writer.handleStatement(vf.createStatement(dataset, RDF.TYPE, vf.createURI(VOID.Dataset.toString())));

        handleSubjects(file);
        handleObjects(file);
        handleProperties(file);

        writer.endRDF();
    }
}
