package tech.dubs.ingest.functions.nlp.simple;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class FilterVocab implements Function<List<String>, List<String>> {
    private final Set<String> vocab;
    private final boolean keep;

    public FilterVocab(Set<String> vocab, boolean keep) {
        this.vocab = vocab;
        this.keep = keep;
    }

    public void apply(Record<List<String>> param, ResultCallback<List<String>> callback) {
        List<String> filtered = new ArrayList<>();

        for (String token : param.getValue()) {
            if (this.vocab.contains(token)) {
                if(this.keep){
                    filtered.add(token);
                }
            } else if (!this.keep) {
                filtered.add(token);
            }
        }

        callback.yield(param.withValue(filtered));
    }
}
