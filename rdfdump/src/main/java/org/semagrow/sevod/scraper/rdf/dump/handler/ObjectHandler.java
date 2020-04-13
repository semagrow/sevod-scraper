package org.semagrow.sevod.scraper.rdf.dump.handler;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.RDFHandlerBase;
import org.semagrow.sevod.scraper.rdf.dump.api.PatternExtractor;
import org.semagrow.sevod.scraper.rdf.dump.extractor.TriePatternExtractor;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by antonis on 15/5/2015.
 */
public class ObjectHandler extends RDFHandlerBase {

    final private Logger log = Logger.getLogger(ObjectHandler.class);

    private PatternExtractor Objects = null;
    private List<String> patterns = new ArrayList<>();

    public ObjectHandler(int bound) {
        Objects = new TriePatternExtractor(bound);
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        super.startRDF();
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        log.debug("Handling statement " + st.toString());
        if (st.getObject() instanceof IRI) {
            String str = ((IRI) st.getObject()).toString();
            Objects.addString(str);
        }
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        patterns = new ArrayList<>(Objects.getPatterns());
        log.debug("Found " + patterns.size() + " object patterns " + patterns);
        super.endRDF();
    }

    public List<String> getPatterns() {
        return patterns;
    }

    private void printPrefixes() {
        Collections.sort(patterns);
        for (String s : patterns) {
            System.out.println(s);
        }
    }
}