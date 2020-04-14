package org.semagrow.sevod.scraper.cli;

import org.apache.commons.cli.*;
import org.semagrow.sevod.scraper.cql.CassandraScraper;
import org.semagrow.sevod.scraper.geordf.dump.GeoRdfDumpScraper;
import org.semagrow.sevod.scraper.rdf.dump.RdfDumpScraper;
import org.semagrow.sevod.scraper.sparql.SparqlScraper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Main {

    public static void main(String[] args) throws ParseException, IOException, org.locationtech.jts.io.ParseException {

        Options options = new Options();

        Option rdfdump = new Option(null, "rdfdump", false,
                "input is a RDF file in ntriples format");
        Option geordfdump = new Option(null, "geordfdump", false,
                "input is a geospatial RDF file in ntriples format");
        Option cassandra = new Option(null, "cassandra", false,
                "input is a Cassandra keyspace");
        Option sparql = new Option(null, "sparql", false,
                "input is a SPARQL endpoint");

        Option input = new Option("i", "input", true,
                "input");
        Option output = new Option("o", "output", true,
                "output metadata file in turtle format");
        Option endpoint = new Option("e", "endpoint", true,
                "SPARQL endpoint URL (used for annotation only)");
        Option prefixes = new Option("p", "prefixes", true,
                "List of known URI prefixes (comma-separated)");
        Option graph = new Option("g", "graph", true,
                "Graph (only for SPARQL endpoint)");
        Option polygon = new Option("P", "polygon", true,
                "Known bounding polygon (for geospatial RDF files)");
        Option namespace = new Option("n", "namespace", true,
                "Namespace for URI mappings (only for cassandra)");

        Option help = new Option("h", "help", false, "print this message");

        options.addOption(rdfdump);
        options.addOption(geordfdump);
        options.addOption(cassandra);
        options.addOption(sparql);

        options.addOption(input);
        options.addOption(output);
        options.addOption(endpoint);
        options.addOption(prefixes);
        options.addOption(graph);
        options.addOption(polygon);
        options.addOption(namespace);

        options.addOption(help);

        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args);

        boolean scraped = false;

        if (line.hasOption(rdfdump.getLongOpt())) {

            RdfDumpScraper scraper = new RdfDumpScraper();

            if (line.hasOption(endpoint.getOpt()) || line.hasOption(endpoint.getLongOpt())) {
                String e = line.getOptionValue(endpoint.getOpt());
                scraper.setEndpoint(e);
            }

            if (line.hasOption(prefixes.getOpt()) || line.hasOption(prefixes.getLongOpt())) {
                String s = line.getOptionValue(prefixes.getOpt());
                Set<String> p = new HashSet<>(Arrays.asList(s.split(",")));
                scraper.setKnownPrefixes(p);
            }

            if ((line.hasOption(input.getOpt()) || line.hasOption(input.getLongOpt()))
                    && (line.hasOption(output.getOpt()) || line.hasOption(output.getLongOpt()))) {
                String i = line.getOptionValue(input.getOpt());
                String o = line.getOptionValue(output.getOpt());
                scraper.scrape(i, o);
                scraped = true;
            }
        }

        if (line.hasOption(geordfdump.getLongOpt())) {

            GeoRdfDumpScraper scraper = new GeoRdfDumpScraper();

            if (line.hasOption(endpoint.getOpt()) || line.hasOption(endpoint.getLongOpt())) {
                String e = line.getOptionValue(endpoint.getOpt());
                scraper.setEndpoint(e);
            }

            if (line.hasOption(prefixes.getOpt()) || line.hasOption(prefixes.getLongOpt())) {
                String s = line.getOptionValue(prefixes.getOpt());
                Set<String> p = new HashSet<>(Arrays.asList(s.split(",")));
                scraper.setKnownPrefixes(p);
            }

            if (line.hasOption(polygon.getOpt()) || line.hasOption(polygon.getLongOpt())) {
                String p = line.getOptionValue(polygon.getOpt());
                scraper.setKnownBoundingPolygon(p);
            }

            if ((line.hasOption(input.getOpt()) || line.hasOption(input.getLongOpt()))
                    && (line.hasOption(output.getOpt()) || line.hasOption(output.getLongOpt()))) {
                String i = line.getOptionValue(input.getOpt());
                String o = line.getOptionValue(output.getOpt());
                scraper.scrape(i, o);
                scraped = true;
            }

        }

        if (line.hasOption(cassandra.getLongOpt())) {

            CassandraScraper scraper = new CassandraScraper();

            if (line.hasOption(namespace.getOpt()) || line.hasOption(namespace.getLongOpt())) {
                String b = line.getOptionValue(namespace.getOpt());
                scraper.setBase(b);
            }

            if ((line.hasOption(input.getOpt()) || line.hasOption(input.getLongOpt()))
                    && (line.hasOption(output.getOpt()) || line.hasOption(output.getLongOpt()))) {
                String i = line.getOptionValue(input.getOpt());
                String o = line.getOptionValue(output.getOpt());
                scraper.scrape(i, o);
                scraped = true;
            }
        }

        if (line.hasOption(sparql.getLongOpt())) {

            SparqlScraper scraper = new SparqlScraper();

            if (line.hasOption(graph.getOpt()) || line.hasOption(graph.getLongOpt())) {
                String b = line.getOptionValue(graph.getOpt());
                scraper.setBaseGraph(b);
            }

            if (line.hasOption(prefixes.getOpt()) || line.hasOption(prefixes.getLongOpt())) {
                String s = line.getOptionValue(prefixes.getOpt());
                Set<String> p = new HashSet<>(Arrays.asList(s.split(",")));
                scraper.setKnownPrefixes(p);
            }

            if ((line.hasOption(input.getOpt()) || line.hasOption(input.getLongOpt()))
                    && (line.hasOption(output.getOpt()) || line.hasOption(output.getLongOpt()))) {
                String i = line.getOptionValue(input.getOpt());
                String o = line.getOptionValue(output.getOpt());
                scraper.scrape(i, o);
                scraped = true;
            }
        }

        if (line.hasOption(help.getOpt()) || line.hasOption(help.getLongOpt()) || !scraped) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setOptionComparator(null);
            formatter.printHelp("sevod_scraper.sh [OPTIONS]...", options);
        }
    }
}
