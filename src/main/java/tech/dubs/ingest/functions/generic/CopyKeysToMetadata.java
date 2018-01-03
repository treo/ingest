package tech.dubs.ingest.functions.generic;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.Map;

public class CopyKeysToMetadata<T, X> implements Function<Map<T, X>, Map<T, X>> {

    private final T[] columnNames;

    public CopyKeysToMetadata(T... columnNames) {
        this.columnNames = columnNames;
    }

    @Override
    public void apply(Record<Map<T, X>> input, ResultCallback<Map<T, X>> callback) {
        Map<T,X> map = input.getValue();
        for (T columnName : columnNames) {
            input.putMeta(columnName, map.get(columnName));
        }
        callback.yield(input.withValue(map));
    }
}
