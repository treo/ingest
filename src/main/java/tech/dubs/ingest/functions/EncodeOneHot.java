package tech.dubs.ingest.functions;

import tech.dubs.ingest.functions.generic.SubsetOfColumnsFunction;

import java.util.List;
import java.util.Map;

public class EncodeOneHot<T> extends SubsetOfColumnsFunction<T, Object, Object> {
    private final Map<T, List<Object>> possibleCategories;

    public EncodeOneHot(Map<T, List<Object>> possibleCategories, T... columnNames) {
        super(columnNames);
        this.possibleCategories = possibleCategories;
    }

    @Override
    protected void parseColumn(Map<T, Object> value, T columnName) {
        Object val = value.get(columnName);
        double[] vector = new double[this.possibleCategories.get(columnName).size()];
        vector[this.possibleCategories.get(columnName).indexOf(val)] = 1.0D;
        value.put(columnName, vector);
    }
}