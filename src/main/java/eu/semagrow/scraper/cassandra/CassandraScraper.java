package eu.semagrow.scraper.cassandra;

import eu.semagrow.scraper.cassandra.utils.CassandraClient;
import org.openrdf.rio.RDFHandlerException;

import java.io.*;

/**
 * Created by antonis on 6/4/2016.
 */
public class CassandraScraper {

    public static void main(String [] args) throws RDFHandlerException, IOException {

        if (args.length != 5) {
            String className = CassandraScraper.class.getName();
            throw new IllegalArgumentException("Usage: " + className + " [address] [port] [keyspace] [base] [sevod output file]");
        }

        String address = args[0];
        int port = Integer.valueOf(args[1]);
        String keyspace = args[2];
        String base = args[3];
        String sevodPath = args[4];

        File sevodFile = new File(sevodPath);
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

        CassandraSchemaMetatataWriter schemaWriter = new CassandraSchemaMetatataWriter();
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
