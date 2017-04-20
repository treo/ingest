package tech.dubs.ingest.functions.nlp.simple;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.Arrays;
import java.util.List;

public class SimpleTokenizer implements Function<String, List<String>> {
    public SimpleTokenizer() {
    }

    public void apply(Record<String> param, ResultCallback<List<String>> callback) {
        List<String> tokens = Arrays.asList(param.getValue().split("[^\\w]+"));
        callback.yield(param.withValue(tokens));
    }
}
