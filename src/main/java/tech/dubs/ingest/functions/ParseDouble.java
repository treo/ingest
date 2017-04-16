package tech.dubs.ingest.functions;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.Map;

public class ParseDouble<T> implements Function<Map<T, Object>, Map<T, Object>> {
    private final T[] columnNames;

    public ParseDouble(T... columnNames) {
        this.columnNames = columnNames;
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
        if(val instanceof String) {
            value.put(columnName, Double.parseDouble((String) val));
        } else {
            throw new IllegalArgumentException("Given column name \"" + columnName + "\" is not a String! It is a " + val.getClass().getCanonicalName());
        }
    }
}
