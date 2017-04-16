package tech.dubs.ingest.functions.generic;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.Map;

public abstract class SubsetOfColumnsFunction<T, X, O> implements Function<Map<T, X>, Map<T, O>> {
    private final T[] columnNames;

    public SubsetOfColumnsFunction(T... columnNames) {
        this.columnNames = columnNames;
    }

    public void apply(Record<Map<T, X>> param, ResultCallback<Map<T, O>> callback) {
        Map<T, O> value = (Map<T, O>) param.getValue();
        if(this.columnNames.length > 0) {
            for (T columnName : this.columnNames) {
                this.parseColumn(value, columnName);
            }
        } else {
            for (T columnName : value.keySet()) {
                this.parseColumn(value, columnName);
            }
        }

        callback.yield(param.withValue(value));
    }

    protected abstract void parseColumn(Map<T, O> value, T columnName);
}