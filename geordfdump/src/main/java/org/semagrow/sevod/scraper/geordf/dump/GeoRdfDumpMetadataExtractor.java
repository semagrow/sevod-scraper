package org.semagrow.sevod.scraper.geordf.dump;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.semagrow.sevod.scraper.geordf.dump.vocabulary.GEO;
import org.semagrow.sevod.scraper.rdf.dump.RdfDumpMetadataExtractor;
import org.semagrow.sevod.scraper.rdf.dump.metadata.Metadata;

import java.util.Set;

public class GeoRdfDumpMetadataExtractor extends RdfDumpMetadataExtractor {

    private Metadata metadata;

    public GeoRdfDumpMetadataExtractor(String endpoint, Set<String> knownPrefixes, Metadata metadata, RDFWriter writer) {
        super(endpoint, knownPrefixes, writer);
        this.metadata = metadata;
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
