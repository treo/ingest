package tech.dubs.ingest.functions.conceptnet;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConceptNetIndexTrieTest {
    @Ignore
    @Test
    public void load() throws Exception {
    }

    @Test
    public void add() throws Exception {
        ConceptNetIndexTrie<Integer> trie = new ConceptNetIndexTrie<>();

        trie.add("/c/en/hello_world", 0);
        trie.add("/c/en/hey", 1);
        trie.add("/c/de/hallo_welten", 2);
        trie.add("/c/de/ich", 3);
        trie.add("/c/en/wurst", 4);
        trie.add("/c/de/hallo_welt", 5);
    }

    @Test
    public void findFirstUnequalChar() throws Exception {
        String a = "Hello World";
        String b = "Hello Paul";

        int firstUnequalChar = ConceptNetIndexTrie.findFirstUnequalChar(a, b);
        assertEquals(6, firstUnequalChar);
    }

}