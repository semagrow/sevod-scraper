package eu.semagrow.stack.metadatagen;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileInputStream;

/**
 * Created by antonis on 14/5/2015.
 */
public class MetadataGeneratorTests extends TestCase {


    public void testSevod() throws Exception {
        MetadataGenerator gen = new MetadataGenerator(15, 350, "http://10.0.100.57:8894/sparql");
        gen.writeMetadata(new File("/home/antonis/datasets/jamendo.nq"), new File("out.n3"));
    }
}
