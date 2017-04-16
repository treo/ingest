package tech.dubs.ingest.functions;

import tech.dubs.ingest.functions.generic.SubsetOfColumnsFunction;

import java.util.Map;

public class ParseDouble<T> extends SubsetOfColumnsFunction<T, String, Object> {
    public ParseDouble(T... columnNames) {
        super(columnNames);
    }

    @Override
    protected void parseColumn(Map<T, Object> value, T columnName) {
        Object val = value.get(columnName);
        if(val instanceof String) {
            value.put(columnName, Double.parseDouble((String) val));
        } else {
            throw new IllegalArgumentException("Given column name \"" + columnName + "\" is not a String! It is a " + val.getClass().getCanonicalName());
        }
    }
}
