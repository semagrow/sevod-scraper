package eu.semagrow.stack.metadatagen.util;

import eu.semagrow.stack.metadatagen.SubjectHandler;
import org.openrdf.rio.*;
import org.openrdf.rio.helpers.RDFHandlerBase;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Created by antonis on 14/5/2015.
 */
public class HandlerBase extends RDFHandlerBase {

    public static void processFile(File file) throws IOException {

        // check for gzip file
        if (file.getName().toLowerCase().contains(".gz")) {
            processInputStream(new GZIPInputStream(new FileInputStream(file)), file.getName());
        }

        // check for zip file
        else if (file.getName().toLowerCase().contains(".zip")) {
            ZipFile zf = new ZipFile(file);
            if (zf.size() > 1) {
                System.err.println("found multiple files in archive, processing only first one.");
            }
            ZipEntry entry = zf.entries().nextElement();
            if (entry.isDirectory()) {
                System.err.println("found directory instead of normal file in archive: " + entry.getName());
                System.exit(1);
            }

            processInputStream(zf.getInputStream(entry), entry.getName());
        }

        // process data stream of file
        else {
            processInputStream(new FileInputStream(file), file.getName());
        }
    }

    public static void processInputStream(InputStream input, String filename) throws IOException {

        long start = System.currentTimeMillis();
        System.err.println("processing " + filename);


        // identify parser format
        RDFFormat format = Rio.getParserFormatForFileName(filename);
        if (format == null) {
            System.err.println("can not identify RDF format for: " + filename);
            System.exit(1);
        }

        // initalize parser
        SubjectHandler handler = new SubjectHandler();
        RDFParser parser = Rio.createParser(format);
        parser.setStopAtFirstError(false);
        parser.setRDFHandler(handler);

        try {
            parser.parse(input, "");
        } catch (RDFParseException e) {
            System.err.println("encountered error while parsing " + filename + ": " + e.getMessage());
            System.exit(1);
        } catch (RDFHandlerException e) {
            System.err.println("encountered error while processing " + filename + ": " + e.getMessage());
            System.exit(1);
        } finally {
            input.close();
        }

        System.err.println((System.currentTimeMillis() - start) / 1000 + " seconds elapsed");
    }

}
