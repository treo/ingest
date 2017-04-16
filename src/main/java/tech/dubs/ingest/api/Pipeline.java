package tech.dubs.ingest.api;

import java.util.Iterator;

public interface Pipeline<T, O> extends Function<T, O> {
    <U> Pipeline<T, U> add(Function<O, U> fn);

    Iterator<Record<O>> apply(Iterator<Record<T>> recordIterator);
}
