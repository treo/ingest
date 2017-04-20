package tech.dubs.ingest.functions.generic;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;
import tech.dubs.ingest.util.CallbackValue;

import java.util.Map;

public class ApplyToKeys<T, O, P, Q> implements Function<Map<T, O>, Map<T, Q>> {

    private final T[] columnNames;
    private final Function<O, P> fn;

    public ApplyToKeys(Function<O, P> fn, T... columnNames) {
        this.columnNames = columnNames;
        this.fn = fn;
    }

    @Override
    public void apply(Record<Map<T, O>> input, ResultCallback<Map<T, Q>> callback) {
        Map<T, O> map = input.getValue();
        Map<T, Q> casted = (Map<T, Q>) map;
        for (T columnName : columnNames) {
            O value = map.get(columnName);
            CallbackValue<P> c = new CallbackValue<>();
            fn.apply(input.withValue(value), c);
            casted.put(columnName, (Q)c.getResult().getValue());
        }
        callback.yield(input.withValue(casted));
    }
}
