package tech.dubs.ingest.functions;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.Map;

public class ConcatColumns<T, O> implements Function<Map<T, O>, Map<T, O>> {
    private final T[] columnNames;
    private final T dest;
    private final String seperator;

    public ConcatColumns(String seperator, T dest, T... columnNames){
        this.seperator = seperator;
        this.dest = dest;
        this.columnNames = columnNames;
    }

    @Override
    public void apply(Record<Map<T, O>> input, ResultCallback<Map<T, O>> callback) {
        StringBuilder builder = new StringBuilder();
        Map<T, O> value = input.getValue();
        for (int i = 0; i < columnNames.length; i++) {
            if(i!=0){
                builder.append(seperator);
            }
            builder.append(value.get(columnNames[i]));
        }
        value.put(dest, (O)builder.toString());
        callback.yield(input.withValue(value));
    }
}
