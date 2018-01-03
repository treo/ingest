package tech.dubs.ingest.functions.generic;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;
import tech.dubs.ingest.util.CallbackValue;

import java.util.Map;

public class ApplyToKeys<T, X, Y> implements Function<Map<T, X>, Map<T, Y>> {

    private final T[] columnNames;
    private final Function fn;

    public ApplyToKeys(Function<?, ?> fn, T... columnNames) {
        this.columnNames = columnNames;
        this.fn = fn;
    }

    @Override
    public void apply(Record<Map<T, X>> input, ResultCallback<Map<T, Y>> callback) {
        Map map = input.getValue();
        for (T columnName : columnNames) {
            Object value = map.get(columnName);
            CallbackValue<?> c = new CallbackValue<>();
            fn.apply(input.withValue(value), c);
            map.put(columnName, c.getResult().getValue());
        }
        callback.yield(input.<Map<T, Y>>withValue(map));
    }
}
