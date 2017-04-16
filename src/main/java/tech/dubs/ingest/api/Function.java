package tech.dubs.ingest.api;

public interface Function<T, O> {
    void apply(Record<T> input, ResultCallback<O> callback);
}
