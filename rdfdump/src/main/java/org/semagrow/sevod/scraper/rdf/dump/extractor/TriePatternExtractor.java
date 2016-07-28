package org.semagrow.sevod.scraper.rdf.dump.extractor;

import org.semagrow.sevod.scraper.rdf.dump.api.PatternExtractor;

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
