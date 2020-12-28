package org.semagrow.sevod.scraper.geordf.dump;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.impl.ValueFactoryImpl;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.semagrow.sevod.scraper.api.Scraper;
import org.semagrow.sevod.scraper.geordf.dump.helpers.WktHelpers;
import org.semagrow.sevod.scraper.rdf.dump.RdfDumpMetadataExtractor;
import org.semagrow.sevod.util.CompactBNodeTurtleWriter;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class GeoRdfDumpScraper implements Scraper {

    private String endpoint = "http://endpoint";
    private Set<String> knownPrefixes = new HashSet<>();
    private Geometry knownBoundingPolygon = null;

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setKnownPrefixes(Set<String> knownPrefixes) {
        this.knownPrefixes = knownPrefixes;
    }

    public void setKnownBoundingPolygon(String knownBoundingPolygon) throws ParseException {
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
}
