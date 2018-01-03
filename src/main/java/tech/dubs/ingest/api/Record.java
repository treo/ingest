package tech.dubs.ingest.api;

import java.util.HashMap;
import java.util.Map;

public class Record<T> {
    private final T value;
    private final Map<Object, Object> meta;

    public Record(T value) {
        this(value, new HashMap());
    }

    private Record(T value, Map<Object, Object> meta) {
        this.value = value;
        this.meta = meta;
    }

    public Record<T> putMeta(Object key, Object value) {
        this.meta.put(key, value);
        return this;
    }

    public Record<T> removeMeta(Object key) {
        this.meta.remove(key);
        return this;
    }

    public <U> U getMeta(Object key, Class<U> clazz) {
        return (U)this.meta.get(key);
    }

    public T getValue() {
        return this.value;
    }

    public <O> Record<O> withValue(O value) {
        return new Record<>(value, new HashMap<>(this.meta));
    }
}
