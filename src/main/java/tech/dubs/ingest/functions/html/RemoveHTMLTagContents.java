package tech.dubs.ingest.functions.html;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

public class RemoveHTMLTagContents implements Function<String, String> {

    private final String[] tagNames;

    public RemoveHTMLTagContents(String... tagNames) {
        this.tagNames = tagNames;
    }

    @Override
    public void apply(Record<String> input, ResultCallback<String> callback) {
        Document doc = Jsoup.parse(input.getValue());
        for (String tagName : tagNames) {
            for (Element element : doc.select(tagName)) {
                element.remove();
            }
        }
        String html = doc.body().html();
        callback.yield(input.withValue(html));
    }
}
