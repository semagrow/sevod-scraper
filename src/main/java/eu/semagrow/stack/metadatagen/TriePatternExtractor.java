package eu.semagrow.stack.metadatagen;

import java.util.Set;

/**
 * Created by antonis on 5/5/2015.
 */
public class TriePatternExtractor implements PatternExtractor {

    PathTrie trie;

    TriePatternExtractor(int bound) {
        trie = new PathTrie(bound);
    }

    @Override
    public Set<String> getPatterns() {
        return trie.getPatterns();
    }
}
