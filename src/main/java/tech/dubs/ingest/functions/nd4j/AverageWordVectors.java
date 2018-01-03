package tech.dubs.ingest.functions.nd4j;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;
import tech.dubs.ingest.functions.conceptnet.ConceptNetWordVectors;

import java.util.List;

public class AverageWordVectors implements Function<List<Integer>, INDArray> {

    private final ConceptNetWordVectors wv;

    public AverageWordVectors(ConceptNetWordVectors wv) {
        this.wv = wv;
    }

    @Override
    public void apply(Record<List<Integer>> input, ResultCallback<INDArray> callback) {
        List<Integer> indexList = input.getValue();
        int[] idxs = new int[indexList.size()];
        for (int i = 0; i < indexList.size(); i++) {
            idxs[i] = indexList.get(i);
        }
        INDArray avg = Nd4j.pullRows(wv.getWordMatrix(), 1, idxs).mean(0);
        callback.yield(input.withValue(avg));
    }
}
