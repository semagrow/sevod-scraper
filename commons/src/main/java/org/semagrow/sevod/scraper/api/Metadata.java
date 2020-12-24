package org.semagrow.sevod.scraper.api;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;

public interface Metadata {
    void serializeMetadata(Resource dataset, RDFWriter writer) throws RDFHandlerException;
}
