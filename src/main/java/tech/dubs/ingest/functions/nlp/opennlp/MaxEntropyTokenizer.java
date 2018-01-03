package tech.dubs.ingest.functions.nlp.opennlp;

import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.Arrays;
import java.util.List;

public class MaxEntropyTokenizer implements Function<String, List<String>> {

    private final TokenizerME tokenizerME;

    public MaxEntropyTokenizer(TokenizerModel model) {
        tokenizerME = new TokenizerME(model);
    }

    @Override
    public void apply(Record<String> input, ResultCallback<List<String>> callback) {
        String value = input.getValue();
        String[] strings = tokenizerME.tokenize(value);
        callback.yield(input.withValue(Arrays.asList(strings)));
    }
}
