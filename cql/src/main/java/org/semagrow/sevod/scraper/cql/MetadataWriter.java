package org.semagrow.sevod.scraper.cql;

import org.semagrow.sevod.scraper.cql.utils.CassandraClient;

import java.io.PrintStream;

/**
 * Created by antonis on 15/4/2016.
 */
public interface MetadataWriter {

    void setClient(CassandraClient client);

    void writeMetadata(PrintStream stream);
}
