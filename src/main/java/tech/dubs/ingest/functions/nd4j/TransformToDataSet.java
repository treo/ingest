package tech.dubs.ingest.functions.nd4j;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TransformToDataSet<T> implements Function<Map<T, Object>, DataSet> {
    private final List<T> featureColumnNames;
    private final List<T> labelColumnNames;

    public TransformToDataSet(List<T> featureColumnNames, List<T> labelColumnNames) {
        this.featureColumnNames = featureColumnNames;
        this.labelColumnNames = labelColumnNames;
    }

    public void apply(Record<Map<T, Object>> param, ResultCallback<DataSet> callback) {
        Map<T, Object> map = param.getValue();
        INDArray features = this.collectToArray(map, this.featureColumnNames);
        INDArray labels = this.collectToArray(map, this.labelColumnNames);
        callback.yield(param.withValue(new DataSet(features, labels)));
    }

    private INDArray collectToArray(Map<T, Object> map, List<T> columnNames) {
        List<INDArray> parts = new ArrayList<>();

        for (T columnName : columnNames) {
            Object val = map.get(columnName);
            if (val instanceof Double) {
                parts.add(Nd4j.create(new double[]{(Double) val}));
            } else {
                if (!(val instanceof double[])) {
                    throw new IllegalArgumentException("Column " + columnName + " is neither a double nor a double[]! It is a " + val.getClass().getCanonicalName());
                }

                parts.add(Nd4j.create((double[]) val));
            }
        }

        return Nd4j.hstack(parts.toArray(new INDArray[0]));
    }
}
