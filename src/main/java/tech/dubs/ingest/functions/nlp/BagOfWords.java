package tech.dubs.ingest.functions.nlp;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BagOfWords implements Function<List<String>, Map<String, Integer>> {
    public BagOfWords() {
    }

    public void apply(Record<List<String>> record, ResultCallback<Map<String, Integer>> callback) {
        Map<String, Integer> map = new HashMap<>();

        for (String token : record.getValue()) {
            Integer val = map.get(token);
            if (val == null) {
                map.put(token, 1);
            } else {
                map.put(token, val + 1);
            }
        }

        callback.yield(record.withValue(map));
    }
}
