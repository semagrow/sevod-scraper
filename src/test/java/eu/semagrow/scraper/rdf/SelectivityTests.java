package eu.semagrow.scraper.rdf;

import junit.framework.TestCase;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by antonis on 24/7/2017.
 */
public class SelectivityTests extends TestCase {

    @Test
    public void testSelectivity() throws Exception {
        ValueFactory vf = ValueFactoryImpl.getInstance();
        Map<URI, Resource> map = new HashMap<>();
        map.put(vf.createURI("http://data.nytimes.com/elements/topicPage"), vf.createBNode());
        map.put(vf.createURI("http://www.w3.org/2002/07/owl#sameAs"), vf.createBNode());
        SelectivityGenerator gen = new SelectivityGenerator(map);
        gen.calculateSelectivities(RDFFormat.NQUADS, new File("/home/antonis/Documents/sevod/NYT.nq"));
    }

    @Test
    public void testSevod() throws Exception {
        String array[] = {"/home/antonis/Documents/sevod/NYT.nq", "http://10.0.100.57:8899/sparql/", "-pvj", "0", "0",  "output.n3"};
        RdfDumpScraper.main(array);
    }
}
