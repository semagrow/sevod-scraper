package eu.semagrow.scraper.cassandra;

import eu.semagrow.scraper.cassandra.utils.CassandraClient;

import java.io.PrintStream;

/**
 * Created by antonis on 15/4/2016.
 */
public interface MetadataWriter {

    void setClient(CassandraClient client);

    void writeMetadata(PrintStream stream);
}
