package eu.semagrow.stack.metadatagen.extractor;

import eu.semagrow.stack.metadatagen.api.PatternExtractor;

import java.util.Set;

/**
 * Created by antonis on 5/5/2015.
 */
public class TriePatternExtractor implements PatternExtractor {

    PathTrie trie;

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
