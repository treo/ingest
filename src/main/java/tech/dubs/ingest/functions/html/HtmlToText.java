package tech.dubs.ingest.functions.html;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

public class HtmlToText implements Function<String, String> {
    @Override
    public void apply(Record<String> input, ResultCallback<String> callback) {
        Document doc = Jsoup.parse(input.getValue());
        String plaintext = doc.body().text();
        callback.yield(input.withValue(plaintext));
    }
}
