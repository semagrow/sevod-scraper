package eu.semagrow.stack.metadatagen;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileInputStream;

/**
 * Created by antonis on 14/5/2015.
 */
public class MetadataGeneratorTests extends TestCase {

    public void testSevod() throws Exception {
        String array[] = {"/home/antonis/datasets/NYT.nq", "http://10.0.100.57:8893/sparql/", "-pv", "0", "0",  "bla.n3"};
        Main.main(array);
    }
}
