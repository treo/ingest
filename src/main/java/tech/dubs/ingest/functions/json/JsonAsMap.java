package tech.dubs.ingest.functions.json;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

public class JsonAsMap<T> implements Function<String, T> {
    private final TypeToken<T> token;

    public JsonAsMap(TypeToken<T> token) {
        this.token = token;
    }

    @Override
    public void apply(Record<String> input, ResultCallback<T> callback) {
        T result = new Gson().fromJson(input.getValue(), token.getType());
        callback.yield(input.withValue(result));
    }
}
