package tech.dubs.ingest.pipelines;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Pipeline;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;
import tech.dubs.ingest.util.CallbackValue;

import java.util.*;

public class SerialPipeline<T, O> implements Pipeline<T, O> {
    private final List<Function<T, O>> functions;

    public SerialPipeline() {
        this(new LinkedList<Function<T, O>>());
    }

    private SerialPipeline(List<Function<T, O>> fns) {
        this.functions = fns;
    }

    public <U> Pipeline<T, U> add(Function<O, U> func) {
        LinkedList<Function> fns = new LinkedList(this.functions);
        fns.add(func);
        return new SerialPipeline(fns);
    }

    public void apply(Record<T> param, ResultCallback<O> callback) {
        List<Record<O>> vals = (List)Collections.singletonList(param);

        for (Function<T, O> fn : this.functions) {
            CallbackValue<O> collector = new CallbackValue<>();
            for (Record<O> record : vals) {
                fn.apply((Record<T>) record, collector);
                vals = collector.getResults();
            }
        }

        for (Record<O> record : vals) {
            callback.yield(record);
        }
    }

    public Iterator<Record<O>> apply(final Iterator<Record<T>> source) {
        return new Iterator<Record<O>>() {
            Deque<Record<O>> queue = new LinkedList<>();

            public boolean hasNext() {
                return source.hasNext() || this.queue.size() > 0;
            }

            public Record<O> next() {
                if(this.queue.size() == 0) {
                    apply(source.next(), new ResultCallback<O>() {
                        public void yield(Record<O> value) {
                            queue.add(value);
                        }
                    });
                }

                return this.queue.pop();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
