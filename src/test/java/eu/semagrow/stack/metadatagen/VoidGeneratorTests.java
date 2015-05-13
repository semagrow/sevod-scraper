package eu.semagrow.stack.metadatagen;

import junit.framework.TestCase;

/**
 * Created by antonis on 28/4/2015.
 */
public class VoidGeneratorTests extends TestCase {

    /*public void testTrie() throws Exception {
        PathTrie trie = new PathTrie(10);

        BufferedReader reader = new BufferedReader(new FileReader("file.txt"));
        String line;
        while ((line = reader.readLine()) != null)
        {
            String str = line.substring(7);
            trie.addPath(str);
        }
        reader.close();
        List<String> list = trie.getPatterns();
        Collections.sort(list);

        for (String s : list) {
            System.out.println(s);
        }
    }*/

    public void testSevod() throws Exception {
        String[] args = {"http://10.0.100.57:8894/sparql", "/home/antonis/datasets/jamendo/output000001.nq"};
        VoidGenerator.main(args);
    }
}
