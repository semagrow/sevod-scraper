package org.semagrow.sevod.scraper.geordf.dump;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.semagrow.sevod.scraper.geordf.dump.metadata.BoundingBoxMetadata;
import org.semagrow.sevod.scraper.rdf.dump.RdfDumpMetadataExtractor;

import java.util.Set;

public class GeoRdfDumpMetadataExtractor extends RdfDumpMetadataExtractor {

    private BoundingBoxMetadata metadata;
    private boolean allSubjectsInMBB = true;

    public GeoRdfDumpMetadataExtractor(String endpoint, Set<String> knownPrefixes, BoundingBoxMetadata metadata, RDFWriter writer) {
        super(endpoint, knownPrefixes, writer);
        this.metadata = metadata;
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        super.startRDF();
        metadata.serializePreamble(this.writer);
    }

    @Override
    public void handleStatement(Statement st) {
        super.handleStatement(st);
        if (allSubjectsInMBB && !metadata.isGeometricURI((URI) st.getSubject())) {
            allSubjectsInMBB = false;
        }
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        super.endRDF();
        if (allSubjectsInMBB) {
            metadata.serializeMetadata(this.dataset, this.writer);
        }
    }
}
