package org.semagrow.sevod.scraper.rdf.dump.legacy.handler;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.RDFHandlerBase;
import org.semagrow.sevod.scraper.rdf.dump.extractor.PatternExtractor;
import org.semagrow.sevod.scraper.rdf.dump.extractor.TriePatternExtractor;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by antonis on 14/5/2015.
 */
@Deprecated
public class SubjectHandler extends RDFHandlerBase {

    final private Logger log = Logger.getLogger(SubjectHandler.class);

    private PatternExtractor Subjects = null;
    private List<String> patterns = new ArrayList<>();

    public SubjectHandler(int bound) {
        Subjects = new TriePatternExtractor(bound);
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        super.startRDF();
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        log.debug("Handling statement " + st.toString());
        if (st.getSubject() instanceof IRI) {
            String str = ((IRI) st.getSubject()).toString();
            Subjects.addString(str);
        }
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        patterns = new ArrayList<>(Subjects.getPatterns());
        log.debug("Found " + patterns.size() + " subject patterns " + patterns);
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
