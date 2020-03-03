package org.semagrow.sevod.scraper.rdf.dump.metadata;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;

public interface Metadata {
    void processStatement(Statement statement);
    void serializeMetadata(Resource dataset, RDFWriter writer) throws RDFHandlerException;
}
