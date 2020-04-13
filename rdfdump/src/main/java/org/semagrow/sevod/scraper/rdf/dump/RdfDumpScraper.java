package org.semagrow.sevod.scraper.rdf.dump;

import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
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
                "\n\t java " + className + " [input_file] [endpoint] [output_file]" +
                "\n\t java " + className + " [input_file] [endpoint] [known_prefixes_path] [output_file]";

        if (args.length > 6 || args.length < 3) {
            throw new IllegalArgumentException(usage);
        }

        File infile = new File(args[0]);
        if (!infile.exists()) {
            throw new RuntimeException("file not found: " + infile);
        }
        if (!infile.isFile()) {
            throw new RuntimeException("not a normal file: " + infile);
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

            generator.setFormat(RDFFormat.NTRIPLES);

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

            String metadataPath = args.length == 3 ? args[2] : args[3];
            Set<String> knownPrefixes = new HashSet<>();

            if (args.length > 3) {
                String prefixesPath = args[2];
                BufferedReader bufferedReader = new BufferedReader(new FileReader(prefixesPath));
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    knownPrefixes.add(line);
                }
                bufferedReader.close();
            }

            RDFWriter writer = new CompactBNodeTurtleWriter(new FileWriter(metadataPath));

            writer.startRDF();

            RdfDumpMetadataExtractor extractor = new RdfDumpMetadataExtractor(endpoint, knownPrefixes, writer);

            RDFParser parser = Rio.createParser(RDFFormat.NTRIPLES);
            parser.getParserConfig().set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
            parser.getParserConfig().set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
            parser.setRDFHandler(extractor);
            parser.parse(new FileInputStream(infile), "");

            writer.endRDF();
        }
    }

}
