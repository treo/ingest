package tech.dubs.ingest.api;

public interface ResultCallback<O> {
    void yield(Record<O> record);
}
