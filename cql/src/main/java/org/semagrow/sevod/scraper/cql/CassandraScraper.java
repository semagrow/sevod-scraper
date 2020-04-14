package org.semagrow.sevod.scraper.cql;

import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.semagrow.sevod.scraper.cql.utils.CassandraClient;

import java.io.*;

/**
 * Created by antonis on 6/4/2016.
 */
public class CassandraScraper {

    private String base = "http://iit.demokritos.gr/cassandra";

    public void setBase(String base) {
        this.base = base;
    }

    public void scrape(String input, String output) throws RDFHandlerException, IOException {

        int i = input.indexOf(':');
        int j = input.indexOf('/');

        String address = input.substring(0,i);
        int port = Integer.valueOf(input.substring(i+1,j));
        String keyspace = input.substring(j+1);

        File sevodFile = new File(output);
        PrintStream stream = new PrintStream(sevodFile);

        if (!sevodFile.exists()) {
            sevodFile.createNewFile();
        }

        if (base.endsWith("/")) {
            base = base.substring(0,base.length()-1);
        }
        String base_keyspace = base + "/" + keyspace;

        if (!base_keyspace.contains("cassandra")) {
            throw new IllegalArgumentException("Base should contain the substring \"cassandra\"");
        }

        CassandraClient client = new CassandraClient();
        client.setCredentials(address, port, keyspace);
        client.connect();

        CassandraSchemaMetadataWriter schemaWriter = new CassandraSchemaMetadataWriter();
        CassandraTriplesMetadataWriter sevodWriter = new CassandraTriplesMetadataWriter();

        schemaWriter.setClient(client);
        sevodWriter.setClient(client);

        schemaWriter.setBase(base_keyspace);
        schemaWriter.setEndpoint(base_keyspace);
        sevodWriter.setBase(base_keyspace);
        sevodWriter.setEndpoint(base_keyspace);

        schemaWriter.writeMetadata(stream);
        sevodWriter.writeMetadata(stream);

        client.close();
    }



}
