package tech.dubs.ingest.functions;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.List;

public class EncodeOneHot<T> implements Function<T, double[]> {
    private final List<Object> possibleCategories;

    public EncodeOneHot(List<Object> possibleCategories) {
        this.possibleCategories = possibleCategories;
    }


    @Override
    public void apply(Record<T> input, ResultCallback<double[]> callback) {
        T val = input.getValue();
        double[] vector = new double[this.possibleCategories.size()];
        vector[this.possibleCategories.indexOf(val)] = 1.0D;
        callback.yield(input.withValue(vector));
    }
}