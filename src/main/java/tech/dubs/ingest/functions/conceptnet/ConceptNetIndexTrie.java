package tech.dubs.ingest.functions.conceptnet;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@Deprecated
public class ConceptNetIndexTrie<T> {
    private CharSequence part = null;
    private boolean isWord = false;
    private Map<Character, ConceptNetIndexTrie> children = new HashMap<>();
    private T value = null;

    public ConceptNetIndexTrie() {}

    private ConceptNetIndexTrie(CharSequence partRest, boolean isWord, Map<Character, ConceptNetIndexTrie> children, T value) {
        this.part = partRest;
        this.isWord = isWord;
        this.children = children;
        this.value = value;
    }

    public static ConceptNetIndexTrie load(String path) throws IOException {
        BufferedReader reader = Files.newBufferedReader(Paths.get(path), Charset.forName("UTF-8"));
        String header = reader.readLine();
        String[] split = header.split(" ");
        Integer wordCount = Integer.valueOf(split[0]);

        ConceptNetIndexTrie<Integer> trie = new ConceptNetIndexTrie<>();
        String line;
        int idx = 0;
        while((line = reader.readLine()) != null){
            String[] parts = line.split(" ", 2);
            trie.add(parts[0], idx);
            idx++;
        }

        return trie;
    }

    protected void add(CharSequence word, T value) {
        if(this.part == null){
            this.part = word;
            this.isWord = true;
            this.value = value;
        }else{
            int end = findFirstUnequalChar(this.part, word);
            CharSequence equalSubSeq = word.subSequence(0, end);
            CharSequence wordRest = word.subSequence(end, word.length());
            CharSequence partRest = part.subSequence(end, part.length());

            if(end < this.part.length()){
                HashMap<Character, ConceptNetIndexTrie> restructure = new HashMap<>();
                restructure.put(partRest.charAt(0), new ConceptNetIndexTrie<T>(partRest, isWord, children, this.value));
                this.children = restructure;
                this.value = null;
            }
            addToChild(wordRest, value);
            this.part = equalSubSeq;
            this.isWord = wordRest.length() == 0 || (partRest.length() == 0 && isWord);
            if(wordRest.length() == 0){
                this.value = value;
            }
        }
    }

    protected void addToChild(CharSequence rest, T value) {
        if(rest.length() == 0) return;

        ConceptNetIndexTrie child;
        char firstChar = rest.charAt(0);
        if(children.containsKey(firstChar)){
            child = children.get(firstChar);
        }else{
            child = new ConceptNetIndexTrie();
            children.put(firstChar, child);
        }
        child.add(rest, value);
    }

    protected static int findFirstUnequalChar(CharSequence part, CharSequence word) {
        int maxPos = Math.min(part.length(), word.length());
        for (int i = 0; i < maxPos; i++) {
            if(part.charAt(i) != word.charAt(i)){
                return i;
            }
        }
        return maxPos;
    }

    // Turn a token list into an index list, multiple tokens may be consumed to produce a single index
    // if a token is out of vocabulary, we will first try to see if it is an english word, and failing that
    // we will try to find a word that shares the same prefix as the given word.
    public List<T> search(List<String> tokens, String languageCode){
        List<T> result = new ArrayList<T>();
        LinkedList<String> tokenList = new LinkedList<>(tokens);

        while(tokenList.size() > 0){
            result.add(search("", "/c/"+languageCode+"/", tokenList));
        }

        return result;
    }

    private T search(String foundSoFar, String missingSuffix, LinkedList<String> restTokens) {
        int i = findFirstUnequalChar(this.part, missingSuffix);

        return null;
    }
}
