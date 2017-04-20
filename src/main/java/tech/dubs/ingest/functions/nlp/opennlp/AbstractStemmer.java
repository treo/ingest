package tech.dubs.ingest.functions.nlp.opennlp;

import opennlp.tools.stemmer.Stemmer;
import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.LinkedList;
import java.util.List;

public abstract class AbstractStemmer implements Function<Object, Object> {
    @Override
    public void apply(Record<Object> input, ResultCallback<Object> callback) {
        Object value = input.getValue();
        if(!(value instanceof List)){
            throw new IllegalArgumentException("Expecting a token list!");
        }

        List<String> lValue = (List<String>) value;
        List<String> results = new LinkedList<>();

        Stemmer stemmer = getStemmer();
        for (String token : lValue) {
            String stemmed = stemmer.stem(token).toString();
            results.add(stemmed);
        }

        callback.yield(input.withValue((Object)results));
    }

    protected abstract Stemmer getStemmer();
}
