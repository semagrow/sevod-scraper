package org.semagrow.sevod.scraper.geordf.dump;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.semagrow.sevod.scraper.geordf.dump.metadata.BoundingBoxMetadata;

public class GeoRdfDumpBoundingBoxExtractor extends RDFHandlerBase {

    private BoundingBoxMetadata metadata;

    public GeoRdfDumpBoundingBoxExtractor(BoundingBoxMetadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        metadata.processStatement(st);
    }
}
