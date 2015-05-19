package eu.semagrow.stack.metadatagen;

import java.io.File;
import java.io.FileInputStream;

/**
 * Created by antonis on 14/5/2015.
 */
public class Main {

    public static void main(String[] args) throws Exception {

        MetadataGenerator generator = null;

        if (args.length != 6 || (args.length == 6 && !args[2].startsWith("-"))) {
            String className = MetadataGenerator.class.getName();
            System.err.println("USAGE: java " + className + " [input_file.nq] [endpoint_url] [-s|p|o] [subjectBound] [objectBound] [output_file.n3]");
            System.exit(1);
        }

        File infile = new File(args[0]);
        if (!infile.exists()) {
            System.err.println("file not found: " + infile);
            System.exit(1);
        }
        if (!infile.isFile()) {
            System.err.println("not a normal file: " + infile);
            System.exit(1);
        }

        String endpoint = args[1];

        String whatToGenerate = args[2];

        int subjectBound = Integer.valueOf(args[3]);
        int objectBound = Integer.valueOf(args[4]);

        File outfile = new File(args[5]);
        if (outfile.exists() && !outfile.isFile()) {
            System.err.println("file not found: " + infile);
            System.exit(1);
        }

        ///////////////////////////////////////////////////////////////////////

        generator = new MetadataGenerator(endpoint);

        if (whatToGenerate.contains("s"))
            generator.generateSubjects();

        if (whatToGenerate.contains("p"))
            generator.generateProperties();

        if (whatToGenerate.contains("o"))
            generator.generateObjects();

        generator.setBounds(subjectBound, objectBound);

        generator.writeMetadata(infile,outfile);
    }
}
