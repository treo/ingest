package tech.dubs.ingest.functions.nlp.simple;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.regex.Pattern;

public class ReplaceMatching implements Function<String, String> {

    private final String replace;
    private final Pattern pattern;

    public ReplaceMatching(String regex, String replace) {
        pattern = Pattern.compile(regex);
        this.replace = replace;
    }

    @Override
    public void apply(Record<String> input, ResultCallback<String> callback) {
        String inputString = input.getValue();
        String output = pattern.matcher(inputString).replaceAll(replace);
        callback.yield(input.withValue(output));
    }
}
