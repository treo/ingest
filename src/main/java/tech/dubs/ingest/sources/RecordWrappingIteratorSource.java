package tech.dubs.ingest.sources;

import tech.dubs.ingest.api.Record;

import java.util.Iterator;

public class RecordWrappingIteratorSource<T> implements Iterator<Record<T>> {
    private final Iterator<T> iterator;

    public RecordWrappingIteratorSource(Iterator<T> iterator) {
        this.iterator = iterator;
    }

    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    public Record<T> next() {
        return new Record<>(this.iterator.next());
    }

    public void remove() {
        this.iterator.remove();
    }
}
