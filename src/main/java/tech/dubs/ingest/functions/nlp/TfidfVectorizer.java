package tech.dubs.ingest.functions.nlp;

import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Pipeline;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class TfidfVectorizer implements Function<Map<String, Integer>, Map<String, Double>> {
    private final Map<String, Integer> docFreq;
    private final double documentCount;

    public TfidfVectorizer(Map<String, Integer> documentFrequencies, int documentCount) {
        this.docFreq = documentFrequencies;
        this.documentCount = (double)documentCount;
    }

    public static <T> TfidfVectorizer create(Iterator<Record<T>> source, Pipeline<T, Map<String, Integer>> pipe, double minDocFreq, double maxDocFreq) {
        Iterator<Record<Map<String, Integer>>> iterator = pipe.apply(source);
        HashMap<String, Integer> docFreq = new HashMap<>();
        int documentCount = 0;

        while(iterator.hasNext()) {
            ++documentCount;
            Record<Map<String, Integer>> record = iterator.next();
            Map<String, Integer> value = record.getValue();

            for (String token : value.keySet()) {
                Integer count = docFreq.get(token);
                if (count == null) {
                    docFreq.put(token, 1);
                } else {
                    docFreq.put(token, count + 1);
                }
            }
        }

        if(minDocFreq < 1.0D) {
            minDocFreq = (double)documentCount * minDocFreq;
        }

        if(maxDocFreq < 1.0D) {
            maxDocFreq = (double)documentCount * maxDocFreq;
        }

        for (String token : docFreq.keySet()) {
            int count = docFreq.get(token);
            if(count > maxDocFreq || count < minDocFreq){
                docFreq.remove(token);
            }
        }
        return new TfidfVectorizer(docFreq, documentCount);
    }

    public Map<String, Integer> getDocFreq() {
        return this.docFreq;
    }

    public void apply(Record<Map<String, Integer>> param, ResultCallback<Map<String, Double>> callback) {
        Map<String, Double> res = new HashMap<>();
        Map<String, Integer> bow = param.getValue();


        int docLength = 0;
        for (int appearanceCount : bow.values()) {
            docLength += appearanceCount;
        }

        for(Entry<String, Integer> entry: bow.entrySet()) {
            String token = entry.getKey();
            Integer docCount = this.docFreq.get(token);
            if(docCount != null) {
                double tf = (double) entry.getValue() / docLength;
                double idf = Math.log(1.0D + this.documentCount / (double)(1 + docCount));
                double tfidf = tf * idf;
                res.put(token, tfidf);
            }
        }

        callback.yield(param.withValue(res));
    }
}
