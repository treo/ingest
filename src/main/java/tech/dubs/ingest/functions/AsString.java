package tech.dubs.ingest.functions;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class AsString implements Function<byte[], String> {
    private final Charset charset;

    public AsString() {
        this(StandardCharsets.UTF_8);
    }

    public AsString(Charset charset) {
        this.charset = charset;
    }

    public void apply(Record<byte[]> param, ResultCallback<String> callback) {
        callback.yield(param.withValue(new String(param.getValue(), this.charset)));
    }
}
