package eu.semagrow.scraper.rdf;

import junit.framework.TestCase;

/**
 * Created by antonis on 14/5/2015.
 */
public class MetadataGeneratorTests extends TestCase {

    public void testSevod() throws Exception {
        String array[] = {"/home/antonis/datasets/SWDF.nq", "http://10.0.100.57:8899/sparql/", "-pv", "0", "0",  "bla.n3"};
        RdfDumpScraper.main(array);
    }
}
