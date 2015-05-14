package eu.semagrow.stack.metadatagen;

import java.io.File;
import java.io.FileInputStream;

/**
 * Created by antonis on 14/5/2015.
 */
public class Main {

    static MetadataGenerator generator = new MetadataGenerator();

    public static void main(String[] args) throws Exception {

        // check for file parameter
        if (args.length < 1) {
            String className = MetadataGenerator.class.getName();
            System.err.println("USAGE: java " + className + " endpoint RDF.nt{.zip}");
            System.exit(1);
        }

        File file = new File(args[0]);
        if (!file.exists()) {
            System.err.println("file not found: " + file);
            System.exit(1);
        }

        // check if file is not a directory
        if (!file.isFile()) {
            System.err.println("not a normal file: " + file);
            System.exit(1);
        }

        generator.writeMetadata(file);

    }
}
