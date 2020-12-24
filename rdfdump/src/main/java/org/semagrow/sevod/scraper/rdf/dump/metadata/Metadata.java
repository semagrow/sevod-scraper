package org.semagrow.sevod.scraper.rdf.dump.metadata;

import org.eclipse.rdf4j.model.Statement;

public interface Metadata extends org.semagrow.sevod.scraper.api.Metadata {
    void processStatement(Statement statement);
}
