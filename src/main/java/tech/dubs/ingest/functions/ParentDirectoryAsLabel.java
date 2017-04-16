package tech.dubs.ingest.functions;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.nio.file.Path;

public class ParentDirectoryAsLabel<T> implements Function<T, T> {
    public ParentDirectoryAsLabel() {
    }

    public void apply(Record<T> param, ResultCallback<T> callback) {
        Path path = param.getMeta("path", Path.class);
        String label = path.toAbsolutePath().getParent().toFile().getName();
        callback.yield(param.putMeta("label", label));
    }
}
