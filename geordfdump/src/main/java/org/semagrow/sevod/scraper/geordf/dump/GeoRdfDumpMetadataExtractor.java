package org.semagrow.sevod.scraper.geordf.dump;

import org.locationtech.jts.geom.Geometry;
import org.openrdf.model.Statement;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.semagrow.sevod.scraper.geordf.dump.metadata.BoundingBoxMetadata;
import org.semagrow.sevod.scraper.geordf.dump.metadata.KnownBoundingPolygonMetadata;
import org.semagrow.sevod.scraper.geordf.dump.vocabulary.GEO;
import org.semagrow.sevod.scraper.rdf.dump.RdfDumpMetadataExtractor;
import org.semagrow.sevod.scraper.rdf.dump.metadata.Metadata;

import java.util.Set;

public class GeoRdfDumpMetadataExtractor extends RdfDumpMetadataExtractor {

    private Metadata metadata;

    public GeoRdfDumpMetadataExtractor(String endpoint, Set<String> knownPrefixes, Geometry knownBoundingPolygon, RDFWriter writer) {
        super(endpoint, knownPrefixes, writer);
        if (knownBoundingPolygon == null) {
            metadata = new BoundingBoxMetadata();
        }
        else {
            metadata = new KnownBoundingPolygonMetadata(knownBoundingPolygon);
        }
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        super.startRDF();
        writer.handleNamespace(RDFS.PREFIX, RDFS.NAMESPACE);
        writer.handleNamespace(GEO.PREFIX, GEO.NAMESPACE);
    }

    @Override
    public void handleStatement(Statement st) {
        super.handleStatement(st);
        metadata.processStatement(st);
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        super.endRDF();
        metadata.serializeMetadata(this.dataset, this.writer);
    }
}