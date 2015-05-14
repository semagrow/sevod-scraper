package eu.semagrow.stack.metadatagen;

import eu.semagrow.stack.metadatagen.util.CompactBNodeTurtleWriter;
import org.openrdf.model.BNode;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;

/**
 * Created by antonis on 14/5/2015.
 */
public class MetadataGenerator {

    //private

    void handleSubjects() {

    }

    // ------------------------------------------------------------

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

        ValueFactory vf = ValueFactoryImpl.getInstance();
        BNode dataset = vf.createBNode();
        RDFWriter writer = new CompactBNodeTurtleWriter(System.out);

        //RDFFormat format = Rio.getParserFormatForFileName(args[1]);
        RDFFormat format = RDFFormat.NQUADS;
        if (format == null) {
            System.err.println("can not identify RDF format for: " + file);
            System.exit(1);
        }

        SubjectHandler subjecthandler = new SubjectHandler();
        RDFParser parser = Rio.createParser(format);
        parser.setRDFHandler(subjecthandler);
        parser.parse(new FileInputStream(file), "");

        List<String> list = subjecthandler.getPatterns();

        SubjectWriter subjectwriter = new SubjectWriter(list);
        parser.setRDFHandler(subjectwriter);
        parser.parse(new FileInputStream(file), "");

        writer.startRDF();
        subjectwriter.writeSevodStats(writer, dataset);
        writer.endRDF();
    }
}
