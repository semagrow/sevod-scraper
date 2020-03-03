package org.semagrow.sevod.scraper.geordf.dump;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.*;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.semagrow.sevod.scraper.geordf.dump.metadata.BoundingBoxMetadata;
import org.semagrow.sevod.scraper.rdf.dump.RdfDumpMetadataExtractor;
import org.semagrow.sevod.util.CompactBNodeTurtleWriter;

import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class GeoRdfDumpScraper {

    private ValueFactory vf = ValueFactoryImpl.getInstance();

    private String endpoint;
    private Set<String> knownPrefixes;
    private Set<String> hasGeomProps;
    private Set<String> asWktProps;

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setKnownPrefixes(String knownPrefixes) throws IOException {
        this.knownPrefixes = fileToSetOfStrings(knownPrefixes);
    }

    public void setHasGeomProps(String hasGeomProps) throws IOException {
        this.hasGeomProps = fileToSetOfStrings(hasGeomProps);
    }

    public void setAsWktProps(String asWktProps) throws IOException {
        this.asWktProps = fileToSetOfStrings(asWktProps);
    }

    public void scrape(String inputPath, String outputPath) throws IOException, RDFHandlerException, RDFParseException {

        Set<URI> hgp = hasGeomProps.stream().map(s -> vf.createURI(s)).collect(Collectors.toSet());
        Set<URI> awp = asWktProps.stream().map(s -> vf.createURI(s)).collect(Collectors.toSet());

        BoundingBoxMetadata boundingBoxMetadata = new BoundingBoxMetadata(hgp, awp);

        GeoRdfDumpBoundingBoxExtractor bboxextractor = new GeoRdfDumpBoundingBoxExtractor(boundingBoxMetadata);

        RDFFormat format = Rio.getParserFormatForFileName(inputPath);
        RDFParser parser = Rio.createParser(format);

        parser.getParserConfig().set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
        parser.setRDFHandler(bboxextractor);
        parser.parse(new FileInputStream(inputPath), "");

        RDFWriter writer = new CompactBNodeTurtleWriter(new FileWriter(outputPath));

        writer.startRDF();

        RdfDumpMetadataExtractor extractor = new GeoRdfDumpMetadataExtractor(endpoint, knownPrefixes, boundingBoxMetadata, writer);

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

        if (args.length > 6 || args.length < 4) {
            throw new IllegalArgumentException();
        }

        GeoRdfDumpScraper scraper = new GeoRdfDumpScraper();

        scraper.setEndpoint(args[1]);
        scraper.setKnownPrefixes(args[2]);
        scraper.setHasGeomProps(args[3]);
        scraper.setAsWktProps(args[4]);

        scraper.scrape(args[0], args[5]);
    }
}
