package org.semagrow.sevod.scraper.sparql.metadata;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;

public interface Metadata {
    void processEndpoint(String endpoint);
    void serializeMetadata(Resource dataset, RDFWriter writer) throws RDFHandlerException;
}
