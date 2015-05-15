package eu.semagrow.stack.metadatagen;

import java.io.File;
import java.io.FileInputStream;

/**
 * Created by antonis on 14/5/2015.
 */
public class Main {

    public static void main(String[] args) throws Exception {

        MetadataGenerator generator = null;

        // check for file parameter
        if (args.length < 5) {
            String className = MetadataGenerator.class.getName();
            System.err.println("USAGE: java " + className + " [input file] [subjectBound] [objectBound] [endpoint url] [output file]");
            System.exit(1);
        }

        File infile = new File(args[0]);
        if (!infile.exists()) {
            System.err.println("file not found: " + infile);
            System.exit(1);
        }

        // check if file is not a directory
        if (!infile.isFile()) {
            System.err.println("not a normal file: " + infile);
            System.exit(1);
        }

        File outfile = new File(args[4]);
        if (!outfile.exists()) {
            System.err.println("file not found: " + outfile);
            System.exit(1);
        }

        // check if file is not a directory
        if (!outfile.isFile()) {
            System.err.println("not a normal file: " + outfile);
            System.exit(1);
        }

        int subjectBound = Integer.valueOf(args[1]);
        int objectBound = Integer.valueOf(args[2]);
        String endpoint = args[3];


        generator = new MetadataGenerator(subjectBound, objectBound, endpoint);
        generator.writeMetadata(infile,outfile);
    }
}
