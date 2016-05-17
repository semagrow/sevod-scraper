package eu.semagrow.scraper.rdf.extractor;

import eu.semagrow.scraper.rdf.api.PatternExtractor;

import java.util.Set;

/**
 * Created by antonis on 5/5/2015.
 */
public class TriePatternExtractor implements PatternExtractor {

    PathTrie trie = null;

    public TriePatternExtractor(int bound) {
        trie = new PathTrie(bound);
    }

    @Override
    public void addString(String string) {
        trie.addPath(string);
    }

    @Override
    public Set<String> getPatterns() {
        return trie.getPatterns();
    }
}
