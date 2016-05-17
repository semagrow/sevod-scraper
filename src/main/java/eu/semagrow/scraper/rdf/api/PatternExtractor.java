package eu.semagrow.scraper.rdf.api;

import java.util.Set;

/**
 * Created by antonis on 5/5/2015.
 */

public interface PatternExtractor {

    void addString(String string);

    Set<String> getPatterns();

}
