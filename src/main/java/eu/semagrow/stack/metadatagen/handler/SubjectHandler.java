package eu.semagrow.stack.metadatagen.handler;

import eu.semagrow.stack.metadatagen.api.PatternExtractor;
import eu.semagrow.stack.metadatagen.extractor.TriePatternExtractor;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by antonis on 14/5/2015.
 */

public class SubjectHandler extends RDFHandlerBase {

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
        if (st.getSubject() instanceof URI) {
            String str = ((URI) st.getSubject()).toString();
            Subjects.addString(str);
        }
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        patterns = new ArrayList<>(Subjects.getPatterns());
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
