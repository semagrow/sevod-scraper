package eu.semagrow.scraper.rdf;

import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.Rio;

import java.io.File;
import java.util.IllegalFormatCodePointException;

/**
 * Created by antonis on 14/5/2015.
 */
public class RdfDumpScraper {

    public static void main(String[] args) throws Exception {

        String className = RdfDumpScraper.class.getName();
        String usage = "USAGE:" +
                "\n\t java " + className + " [input_file.nq] [endpoint_url] [-s|p|o] [output_file.n3]" +
                "\n\t java " + className + " [input_file.nq] [endpoint_url] [-s|p|o] [subjectBound] [objectBound] [output_file.n3]";

        if (args.length != 6 && args.length != 4) {
            throw new IllegalArgumentException(usage);
        }
        if (!args[2].startsWith("-")) {
            throw new IllegalArgumentException(usage);
        }

        File infile = new File(args[0]);
        if (!infile.exists()) {
            throw new RuntimeException("file not found: " + infile);
        }
        if (!infile.isFile()) {
            throw new RuntimeException("not a normal file: " + infile);
        }
        RDFFormat format = Rio.getParserFormatForFileName(args[0]);
        if (format == null) {
            throw new RuntimeException("can not identify RDF format for: " + args[0]);
        }

        String endpoint = args[1];

        String whatToGenerate = args[2];

        int subjectBound = 0;
        int objectBound = 0;
        String fileName = args[3];

        if (args.length == 6) {
            subjectBound = Integer.valueOf(args[3]);
            objectBound = Integer.valueOf(args[4]);
            fileName = args[5];
        }

        File outfile = new File(fileName);
        if (outfile.exists() && !outfile.isFile()) {
            throw new RuntimeException("file not found: " + infile);
        }

        ///////////////////////////////////////////////////////////////////////

        MetadataGenerator generator = new MetadataGenerator(endpoint);

        generator.setFormat(format);

        if (whatToGenerate.contains("s"))
            generator.generateSubjects();

        if (whatToGenerate.contains("p"))
            generator.generateProperties();

        if (whatToGenerate.contains("o"))
            generator.generateObjects();

        if (whatToGenerate.contains("v"))
            generator.generateVocabulary();

        generator.setBounds(subjectBound, objectBound);

        generator.writeMetadata(infile,outfile);
    }
}
