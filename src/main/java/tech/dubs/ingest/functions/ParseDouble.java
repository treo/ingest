package tech.dubs.ingest.functions;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

public class ParseDouble implements Function<String, Double> {
    @Override
    public void apply(Record<String> input, ResultCallback<Double> callback) {
        String value = input.getValue();
        callback.yield(input.withValue(Double.parseDouble(value)));
    }
}
