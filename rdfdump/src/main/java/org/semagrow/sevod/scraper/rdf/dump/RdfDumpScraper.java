package org.semagrow.sevod.scraper.rdf.dump;

import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.semagrow.sevod.util.CompactBNodeTurtleWriter;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by antonis on 14/5/2015.
 */
public class RdfDumpScraper {

    public static void main(String[] args) throws Exception {

        String className = RdfDumpScraper.class.getName();
        String usage = "USAGE:" +
                "\n\t java " + className + " [input_file] [endpoint] [-s|p|o|v] [output_file]" +
                "\n\t java " + className + " [input_file] [endpoint] [-s|p|o|v] [subjectBound] [objectBound] [output_file]" +
                "\n\t java " + className + " [input_file] [endpoint] [known_prefixes_path] [output_file]";

        if (args.length > 6 || args.length < 4) {
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

        if (args[2].startsWith("-")) {

            // old functionality

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

            if (whatToGenerate.contains("j"))
                generator.generateSelectivities();

            if (whatToGenerate.contains("E"))
                generator.useEndpoint();

            generator.setBounds(subjectBound, objectBound);

            generator.writeMetadata(infile,outfile);
        }
        else {
            // new functionality (known prefixes)

            String prefixesPath = args[2];
            String metadataPath = args[3];

            BufferedReader bufferedReader = new BufferedReader(new FileReader(prefixesPath));

            String line;
            Set<String> knownPrefixes = new HashSet<>();

            while ((line = bufferedReader.readLine()) != null) {
                knownPrefixes.add(line);
            }

            bufferedReader.close();

            RDFWriter writer = new CompactBNodeTurtleWriter(new FileWriter(metadataPath));

            writer.startRDF();

            RdfDumpMetadataExtractor extractor = new RdfDumpMetadataExtractor(endpoint, knownPrefixes, writer);

            RDFParser parser = Rio.createParser(format);
            parser.getParserConfig().set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
            parser.setRDFHandler(extractor);
            parser.parse(new FileInputStream(infile), "");

            writer.endRDF();
        }
    }

}
