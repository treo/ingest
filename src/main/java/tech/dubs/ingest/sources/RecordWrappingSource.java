package tech.dubs.ingest.sources;

import tech.dubs.ingest.api.Record;

import java.util.Iterator;

public class RecordWrappingSource<T> implements Iterable<Record<T>> {
    private final Iterable<T> iterable;

    public RecordWrappingSource(Iterable<T> iterable) {
        this.iterable = iterable;
    }

    public Iterator<Record<T>> iterator() {
        return new RecordWrappingIteratorSource(this.iterable.iterator());
    }
}
