package org.semagrow.sevod.scraper.rdf.dump.extractor;

/**
 * Created by antru on 29/4/2015.
 */

import java.util.*;


public class PathTrie {

    private final TrieNode rootNode ;
    public int bound;
    static final String kleeneStar = "*";

    static class TrieNode {

        boolean property = false;
        boolean pruned = false;
        final HashMap<String, TrieNode> children;
        TrieNode parent = null;

        private TrieNode(TrieNode parent) {
            children = new HashMap<String, TrieNode>();
            this.parent = parent;
            pruned = false;
        }

        TrieNode getParent() {
            return this.parent;
        }

        void setParent(TrieNode parent) {
            this.parent = parent;
        }

        void setProperty(boolean prop) {
            this.property = prop;
        }

        boolean getProperty() {
            return this.property;
        }

        boolean isPruned() { return this.pruned; }

        /**
         * add a child to the existing node, iff the node is not full
         * @param childName the string name of the child
         * @param node the node that is the child
         */

        void addChild(String childName, TrieNode node) {
            synchronized(children) {
                if (children.containsKey(childName)) {
                    return;
                }
                children.put(new String(childName), node);
            }
        }

        /**
         * delete child from this node
         * @param childName the string name of the child to
         * be deleted
         */
        void deleteChild(String childName) {
            synchronized(children) {
                if (!children.containsKey(childName)) {
                    return;
                }
                TrieNode childNode = children.get(childName);
                // this is the only child node.
                if (childNode.getChildren().size() == 1) {
                    childNode.setParent(null);
                    children.remove(childName);
                }
                else {
                    // their are more child nodes
                    // so just reset property.
                    childNode.setProperty(false);
                }
            }
        }

        /**
         * return the child of a node mapping
         * to the input childname
         * @param childName the name of the child
         * @return the child of a node
         */
        TrieNode getChild(String childName) {
            synchronized(children) {
                if (!children.containsKey(childName)) {
                    return null;
                }
                else {
                    return children.get(childName);
                }
            }
        }

        /**
         * get the list of children of this
         * trienode.
         * @return the string list of its children
         */
        Set<String> getChildren() {
            synchronized(children) {
                return children.keySet();
            }
        }

        /**
         * get the string representation
         * for this node
         */
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Children of trienode: ");
            synchronized(children) {
                for (String str: children.keySet()) {
                    sb.append(" " + str);
                }
            }
            return sb.toString();
        }

        /**
         * prune children of this node
         */
        private void pruneNode(int bound) {
            synchronized (children) {
                int count = 0; // number of already pruned children
                int all = getChildren().size(); // number of all children

                if (this.pruned)
                    return;

                for (String key : children.keySet()) {
                    if (getChild(key).isPruned())
                        count++;
                }
                if (count > bound || (count == 0 && all > bound)) {
                    children.clear();
                    addChild(kleeneStar, new TrieNode(this));
                    this.pruned = true;
                }
                if (all - count > bound) {
                    for (String key : children.keySet()) {
                        if (!children.get(key).isPruned())
                            children.remove(key);
                    }
                    addChild(kleeneStar, new TrieNode(this));
                    this.pruned = true;
                }

                for (String key : children.keySet()) {
                    if (key != kleeneStar) {
                        children.get(key).pruneNode(bound);
                    }
                }
            }
        }

        /**
         * get a list of all patterns for this node
         */
        public Set<String> getPatterns() {
            Set<String> patterns = new HashSet<>();
            synchronized (children) {
                for (String str : children.keySet()) {
                    if (children.get(str).getPatterns().isEmpty()) {
                        patterns.add(str);
                    }
                    for (String childPattern : children.get(str).getPatterns() ) {
                        patterns.add(str + "/" + childPattern);
                    }
                }
                if (this.isPruned())
                    patterns.add("*");
            }
            return patterns;
        }
    }

    /**
     * construct a new PathTrie with
     * a root node of /
     */
    public PathTrie(int b) {
        this.bound = b;
        this.rootNode = new TrieNode(null);
    }

    /**
     * add a path to the path trie
     * @param path
     */
    public void addPath(String path) {
        if (path == null) {
            return;
        }
        List<String> pathComponents = new ArrayList<>(Arrays.asList(path.split("/")));

        if (pathComponents.size() < 1) {
            throw new IllegalArgumentException("Invalid path " + path);
        }

        TrieNode parent = rootNode;
        if (pathComponents.size() > 1) {
            pathComponents.set(pathComponents.size() - 1, kleeneStar);
        }

        for (String part : pathComponents) {
            if (parent.getChild(part) == null) {
                parent.addChild(part, new TrieNode(parent));
            }
            parent = parent.getChild(part);
        }
        parent.setProperty(true);
    }

    /**
     * get all patterns of PathTrie
     */

    public Set<String> getPatterns() {
        this.rootNode.pruneNode(bound);
        Set<String> set = new HashSet<>();
        for (String s : this.rootNode.getPatterns()) {
            set.add(s.substring(0,s.length()-1));
        }
        return set;
    }

}
