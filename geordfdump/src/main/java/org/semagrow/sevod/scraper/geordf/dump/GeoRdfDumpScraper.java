package org.semagrow.sevod.scraper.geordf.dump;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.impl.ValueFactoryImpl;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.semagrow.sevod.scraper.geordf.dump.helpers.WktHelpers;
import org.semagrow.sevod.scraper.rdf.dump.RdfDumpMetadataExtractor;
import org.semagrow.sevod.util.CompactBNodeTurtleWriter;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class GeoRdfDumpScraper {

    private String endpoint;
    private Set<String> knownPrefixes;
    private Geometry knownBoundingPolygon = null;

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setKnownPrefixes(String knownPrefixes) throws IOException {
        this.knownPrefixes = fileToSetOfStrings(knownPrefixes);
    }

    public void setKnownBoundingPolygon(String knownBoundingPolygonPath) throws IOException, ParseException {
        String knownBoundingPolygon = fileToSetOfStrings(knownBoundingPolygonPath).iterator().next();
        Literal l = NTriplesUtil.parseLiteral(knownBoundingPolygon, ValueFactoryImpl.getInstance());
        this.knownBoundingPolygon = WktHelpers.createGeometry(l, WktHelpers.getCRS(l));
    }

    public void scrape(String inputPath, String outputPath) throws IOException, RDFHandlerException, RDFParseException {

        RDFWriter writer = new CompactBNodeTurtleWriter(new FileWriter(outputPath));

        writer.startRDF();

        RdfDumpMetadataExtractor extractor = new GeoRdfDumpMetadataExtractor(endpoint, knownPrefixes, knownBoundingPolygon, writer);

        RDFFormat format = RDFFormat.NTRIPLES;
        RDFParser parser = Rio.createParser(format);
        parser.getParserConfig().set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
        parser.getParserConfig().set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
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

    public static void main(String[] args) throws IOException, RDFParseException, RDFHandlerException, ParseException {

        if (args.length < 4) {
            throw new IllegalArgumentException();
        }

        GeoRdfDumpScraper scraper = new GeoRdfDumpScraper();

        if (args.length == 4) {
            scraper.setEndpoint(args[1]);
            scraper.setKnownPrefixes(args[2]);

            scraper.scrape(args[0], args[3]);
        }
        else {
            scraper.setEndpoint(args[1]);
            scraper.setKnownPrefixes(args[2]);
            scraper.setKnownBoundingPolygon(args[3]);

            scraper.scrape(args[0], args[4]);
        }
    }
}
