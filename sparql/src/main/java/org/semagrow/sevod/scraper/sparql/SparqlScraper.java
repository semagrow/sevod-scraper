package org.semagrow.sevod.scraper.sparql;

import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.semagrow.sevod.util.CompactBNodeTurtleWriter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by antonis on 29/7/2016.
 */
public class SparqlScraper {

    public static void main(String[] args) throws IOException, RDFHandlerException {
        String className = SparqlScraper.class.getName();
        String usage = "USAGE:" +
                "\n\t java " + className + " [endpoint_url] [base_graph] [known_prefixes] [output_file.n3]";

        if (args.length < 2) {
            throw new IllegalArgumentException(usage);
        }

        String endpoint = args[0];
        String baseGraph = args[1].equals("all") ? null : args[1];
        String prefixesPath = args[2];
        String metadataPath = args[3];

        BufferedReader bufferreader = new BufferedReader(new FileReader(prefixesPath));

        String line;
        List<String> prefixes = new ArrayList<>();

        while ((line = bufferreader.readLine()) != null) {
            prefixes.add(line);
        }

        bufferreader.close();

        RDFWriter writer = new CompactBNodeTurtleWriter(new FileWriter(metadataPath));

        SparqlMetadataExtractor extractor = new SparqlMetadataExtractor(endpoint, baseGraph, prefixes);

        writer.startRDF();
        extractor.writeMetadata(writer);
        writer.endRDF();
    }
}
