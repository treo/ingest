package tech.dubs.ingest.util;

import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CallbackValue<T> implements ResultCallback<T> {
    private final List<Record<T>> results = new ArrayList<>(1);

    public CallbackValue() {
    }

    public void yield(Record<T> value) {
        this.results.add(value);
    }

    public List<Record<T>> getResults() {
        return Collections.unmodifiableList(this.results);
    }

    public Record<T> getResult() {
        if(this.results.size() == 0) {
            throw new IllegalArgumentException("No result available! Callback not called yet!");
        } else if(this.results.size() != 1) {
            throw new IllegalArgumentException("More than one result! Use getResults() method instead!");
        } else {
            return this.results.get(0);
        }
    }
}
