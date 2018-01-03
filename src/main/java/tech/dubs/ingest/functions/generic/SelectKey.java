package tech.dubs.ingest.functions.generic;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.Map;

public class SelectKey<T, X, Y> implements Function<Map<T, X>, Y> {
    private final T key;

    public SelectKey(T key) {
        this.key = key;
    }


    @Override
    public void apply(Record<Map<T, X>> input, ResultCallback<Y> callback) {
        callback.yield(input.withValue((Y)input.getValue().get(key)));
    }
}
