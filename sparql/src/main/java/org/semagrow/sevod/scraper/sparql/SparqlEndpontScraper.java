package org.semagrow.sevod.scraper.sparql;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Created by antonis on 29/7/2016.
 */
public class SparqlEndpontScraper {

    public static void main(String[] args) throws IOException {
        String className = SparqlEndpontScraper.class.getName();
        String usage = "USAGE:" +
                "\n\t java " + className + " [endpoint_url] [output_file.n3]";

        if (args.length != 2) {
            throw new IllegalArgumentException(usage);
        }

        String endpoint = args[0];
        String path = args[1];

        File file = new File(path);
        if (!file.exists()) {
            file.createNewFile();
        }
        PrintStream stream = new PrintStream(file);

        //sevodWriter.writeMetadata(stream);

        stream.close();
    }
}
