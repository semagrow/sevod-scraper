package eu.semagrow.stack.metadatagen;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileInputStream;

/**
 * Created by antonis on 14/5/2015.
 */
public class MetadataGeneratorTests extends TestCase {


    public void testSevod() throws Exception {
        String[] args = {"http://10.0.100.57:8894/sparql", "/home/antonis/datasets/jamendo/output000001.nq"};
        MetadataGenerator gen = new MetadataGenerator();
        gen.writeMetadata(new File("/home/antonis/datasets/jamendo/output000001.nq"));
    }
}
