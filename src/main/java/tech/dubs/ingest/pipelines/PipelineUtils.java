package tech.dubs.ingest.pipelines;

import tech.dubs.ingest.api.Pipeline;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.util.CallbackValue;

import java.util.Iterator;
import java.util.List;

public class PipelineUtils {
    public PipelineUtils() {
    }

    public static <T, O> Record<O> applyPipelineSingle(Pipeline<T, O> pipeline, Record<T> value) {
        CallbackValue<O> callback = new CallbackValue<>();
        pipeline.apply(value, callback);
        return callback.getResult();
    }

    public static <T, O> List<Record<O>> applyPipeline(Pipeline<T, O> pipeline, Record<T> value) {
        CallbackValue<O> callback = new CallbackValue<>();
        pipeline.apply(value, callback);
        return callback.getResults();
    }

    public static <T> Iterator<T> unwrapRecordValues(final Iterator<Record<T>> recordIterator) {
        return new Iterator<T>() {
            public boolean hasNext() {
                return recordIterator.hasNext();
            }

            public void remove() {
                recordIterator.remove();
            }

            public T next() {
                return (T)recordIterator.next().getValue();
            }
        };
    }
}
