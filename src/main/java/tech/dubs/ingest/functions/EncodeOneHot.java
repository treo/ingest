package tech.dubs.ingest.functions;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.List;
import java.util.Map;

public class EncodeOneHot<T> implements Function<Map<T, Object>, Map<T, Object>> {
    private final T[] columnNames;
    private final Map<T, List<Object>> possibleCategories;

    public EncodeOneHot(Map<T, List<Object>> possibleCategories, T... columnNames) {
        this.columnNames = columnNames;
        this.possibleCategories = possibleCategories;
    }

    public void apply(Record<Map<T, Object>> param, ResultCallback<Map<T, Object>> callback) {
        Map<T, Object> value = param.getValue();
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

    private void parseColumn(Map<T, Object> value, T columnName) {
        Object val = value.get(columnName);
        if(val instanceof Integer) {
            double[] vector = new double[this.possibleCategories.get(columnName).size()];
            vector[(Integer) val] = 1.0D;
            value.put(columnName, vector);
        } else {
            throw new IllegalArgumentException("Given column name \"" + columnName + "\" is not an Integer, i.e. not a categorical index! It is a " + val.getClass().getCanonicalName());
        }
    }
}