package tech.dubs.ingest.functions;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.*;
import java.util.Map.Entry;

public class ParseCategorical<T> implements Function<Map<T, Object>, Map<T, Object>> {
    private final T[] columnNames;
    private final Map<T, List<Object>> possibleCategories;

    public ParseCategorical(Map<T, List<Object>> possibleCategories, T... columnKeys) {
        this.columnNames = columnKeys;
        this.possibleCategories = possibleCategories;
    }

    public static <T> Map<T, List<Object>> collectCategories(Iterator<Record<Map<T, Object>>> iterator, T... columnNames) {
        Map<T, Set<Object>> categoriesPerColumn = new HashMap<>();

        while(iterator.hasNext()) {
            Map<T, Object> map = iterator.next().getValue();
            if(columnNames.length == 0) {
                columnNames = map.keySet().toArray(columnNames);
            }

            for (T columnName : columnNames) {
                Object category = map.get(columnName);
                Set<Object> set = categoriesPerColumn.get(columnName);
                if(set == null) {
                    set = new HashSet<>();
                    categoriesPerColumn.put(columnName, set);
                }

                set.add(category);
            }
        }

        HashMap<T, List<Object>> result = new HashMap<>();
        for (Entry<T, Set<Object>> entry : categoriesPerColumn.entrySet()) {
            List<Object> list = new ArrayList<>(entry.getValue());
            if(list.get(0) instanceof Comparable){
                Collections.sort((List)list);
            }
            result.put(entry.getKey(), list);
        }
        return result;
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
        int index = this.possibleCategories.get(columnName).indexOf(value.get(columnName));
        value.put(columnName, index);
    }
}
