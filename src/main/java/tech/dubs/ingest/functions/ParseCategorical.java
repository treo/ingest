package tech.dubs.ingest.functions;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.*;
import java.util.Map.Entry;

public class ParseCategorical<T> implements Function<T,Integer> {
    private final List<T> possibleCategories;

    public ParseCategorical(List<T> possibleCategories) {
        this.possibleCategories = possibleCategories;
    }

    public static <T, K> Map<T, List<K>> collectCategories(Iterator<Record<Map<T, K>>> iterator, T... columnNames) {
        Map<T, Set<K>> categoriesPerColumn = new HashMap<>();

        while(iterator.hasNext()) {
            Map<T, K> map = iterator.next().getValue();
            if(columnNames.length == 0) {
                columnNames = map.keySet().toArray(columnNames);
            }

            for (T columnName : columnNames) {
                K category = map.get(columnName);
                Set<K> set = categoriesPerColumn.get(columnName);
                if(set == null) {
                    set = new HashSet<>();
                    categoriesPerColumn.put(columnName, set);
                }

                set.add(category);
            }
        }

        HashMap<T, List<K>> result = new HashMap<>();
        for (Entry<T, Set<K>> entry : categoriesPerColumn.entrySet()) {
            List<K> list = new ArrayList<>(entry.getValue());
            if(list.get(0) instanceof Comparable){
                Collections.sort((List)list);
            }
            result.put(entry.getKey(), list);
        }
        return result;
    }

    @Override
    public void apply(Record<T> input, ResultCallback<Integer> callback) {
        T value = input.getValue();
        int idx = this.possibleCategories.indexOf(value);
        callback.yield(input.withValue(idx));
    }
}
