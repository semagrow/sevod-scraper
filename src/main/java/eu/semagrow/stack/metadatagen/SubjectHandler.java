package eu.semagrow.stack.metadatagen;

import eu.semagrow.stack.metadatagen.api.PatternExtractor;
import eu.semagrow.stack.metadatagen.extractor.TriePatternExtractor;
import eu.semagrow.stack.metadatagen.util.HandlerBase;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.rio.RDFHandlerException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by antonis on 14/5/2015.
 */

public class SubjectHandler extends HandlerBase {

    private PatternExtractor Subjects = new TriePatternExtractor(15);
    private List<String> patterns = new ArrayList<>();

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        if (st.getSubject() instanceof URI) {
            String str = ((URI) st.getSubject()).toString();
            Subjects.addString(str);
        }
    }

    public List<String> getPatterns() { return patterns; }

    private void printPrefixes() {
        patterns = new ArrayList<>(Subjects.getPatterns());
        Collections.sort(patterns);
        for (String s : patterns) {
            System.out.println(s);
        }
    }
}
