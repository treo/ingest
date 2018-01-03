package tech.dubs.ingest.functions.conceptnet;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class ConceptNetStringRetokenizer implements Function<List<String>, List<String>> {
    private Map<String, Integer> vocab = new HashMap<>();
    private int outOfVocabularyCount = 0;

    public static ConceptNetStringRetokenizer load(String path) throws IOException {
        BufferedReader reader = Files.newBufferedReader(Paths.get(path), Charset.forName("UTF-8"));
        String header = reader.readLine();
        String[] split = header.split(" ");
        Integer wordCount = Integer.valueOf(split[0]);

        ConceptNetStringRetokenizer tokenizer = new ConceptNetStringRetokenizer();
        String line;
        int idx = 0;
        while((line = reader.readLine()) != null){
            String[] parts = line.split(" ", 2);
            tokenizer.vocab.put(parts[0], idx);
            idx++;
        }

        return tokenizer;
    }

    @Override
    public void apply(Record<List<String>> input, ResultCallback<List<String>> callback) {
        List<String> inputTokens = input.getValue();
        Queue<String> value = new LinkedList<>();
        for (String token : inputTokens) {
            value.add(token.toLowerCase());
        }

        List<String> result = new ArrayList<>();
        while(!value.isEmpty()){
            String startToken = "/c/en/" + value.poll();

            if(vocab.containsKey(startToken)){
                String longestToken = findLongestToken(startToken, value);
                result.add(longestToken);
            }else{
                outOfVocabularyCount++;
            }
        }

        if(result.size() > 0) {
            callback.yield(input.withValue(result));
        }else{
            System.out.println("WARN: input " + input.getValue().toString() + " doesn't result in any found tokens! Skipping.");
            callback.yield(input.withValue(Arrays.asList("/c/en/a")));
        }
    }

    private String findLongestToken(String cur, Queue<String> more){
        if(more.size() == 0){ return cur; }

        String next_possible = cur + "_" + more.peek();
        if(vocab.containsKey(next_possible)){
            more.poll();
            return findLongestToken(next_possible, more);
        }else{
            return cur;
        }
    }

    public int getVocabSize(){
        return vocab.size();
    }
}
