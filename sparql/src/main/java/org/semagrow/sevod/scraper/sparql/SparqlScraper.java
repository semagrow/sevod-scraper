package org.semagrow.sevod.scraper.sparql;

import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.semagrow.sevod.scraper.api.Scraper;
import org.semagrow.sevod.util.CompactBNodeTurtleWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by antonis on 29/7/2016.
 */
public class SparqlScraper implements Scraper {

    private String baseGraph = null;
    private Set<String> knownPrefixes = new HashSet<>();

    public void setBaseGraph(String baseGraph) {
        this.baseGraph = baseGraph;
    }

    public void setKnownPrefixes(Set<String> knownPrefixes) {
        this.knownPrefixes = knownPrefixes;
    }

    public void scrape(String endpoint, String metadataPath) throws IOException, RDFHandlerException {

        RDFWriter writer = new CompactBNodeTurtleWriter(new FileWriter(metadataPath));

        SparqlMetadataExtractor extractor = new SparqlMetadataExtractor(endpoint, baseGraph, knownPrefixes);

        writer.startRDF();
        extractor.writeMetadata(writer);
        writer.endRDF();
    }
}
