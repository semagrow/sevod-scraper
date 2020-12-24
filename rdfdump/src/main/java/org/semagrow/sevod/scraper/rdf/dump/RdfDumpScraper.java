package org.semagrow.sevod.scraper.rdf.dump;

import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.semagrow.sevod.scraper.api.Scraper;
import org.semagrow.sevod.util.CompactBNodeTurtleWriter;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class RdfDumpScraper implements Scraper {

    private String endpoint = "http://endpoint";
    private Set<String> knownPrefixes = new HashSet<>();

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setKnownPrefixes(Set<String> knownPrefixes) {
        this.knownPrefixes = knownPrefixes;
    }

    public void scrape(String inputPath, String outputPath) throws IOException, RDFHandlerException, RDFParseException {

        RDFWriter writer = new CompactBNodeTurtleWriter(new FileWriter(outputPath));

        writer.startRDF();

        RdfDumpMetadataExtractor extractor = new RdfDumpMetadataExtractor(endpoint, knownPrefixes, writer);

        RDFFormat format = RDFFormat.NTRIPLES;
        RDFParser parser = Rio.createParser(format);
        parser.getParserConfig().set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
        parser.getParserConfig().set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
        parser.setRDFHandler(extractor);
        parser.parse(new FileInputStream(inputPath), "");

        writer.endRDF();
    }
}
