package org.semagrow.sevod.scraper.geordf.dump;

import org.openrdf.rio.*;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.semagrow.sevod.scraper.rdf.dump.RdfDumpMetadataExtractor;
import org.semagrow.sevod.util.CompactBNodeTurtleWriter;

import java.io.*;
import java.util.HashSet;
import java.util.Set;


public class GeoRdfDumpScraper {

    private String endpoint;
    private Set<String> knownPrefixes;

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setKnownPrefixes(String knownPrefixes) throws IOException {
        this.knownPrefixes = fileToSetOfStrings(knownPrefixes);
    }

    public void scrape(String inputPath, String outputPath) throws IOException, RDFHandlerException, RDFParseException {

        RDFWriter writer = new CompactBNodeTurtleWriter(new FileWriter(outputPath));

        writer.startRDF();

        RdfDumpMetadataExtractor extractor = new GeoRdfDumpMetadataExtractor(endpoint, knownPrefixes, writer);

        RDFFormat format = Rio.getParserFormatForFileName(inputPath);
        RDFParser parser = Rio.createParser(format);
        parser.getParserConfig().set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
        parser.setRDFHandler(extractor);
        parser.parse(new FileInputStream(inputPath), "");

        writer.endRDF();
    }

    private Set<String> fileToSetOfStrings(String path) throws IOException {

        BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
        String line;
        Set<String> set = new HashSet<>();

        while ((line = bufferedReader.readLine()) != null) {
            set.add(line);
        }
        bufferedReader.close();

        return set;
    }

    public static void main(String[] args) throws IOException, RDFParseException, RDFHandlerException {

        if (args.length < 4) {
            throw new IllegalArgumentException();
        }

        GeoRdfDumpScraper scraper = new GeoRdfDumpScraper();

        scraper.setEndpoint(args[1]);
        scraper.setKnownPrefixes(args[2]);

        scraper.scrape(args[0], args[3]);
    }
}
