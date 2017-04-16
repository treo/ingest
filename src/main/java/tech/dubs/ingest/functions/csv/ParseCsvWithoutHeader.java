package tech.dubs.ingest.functions.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ParseCsvWithoutHeader implements Function<String, Map<Integer, String>> {
    private CSVFormat format;
    private boolean skipFirstLine;

    public ParseCsvWithoutHeader(boolean skipFirstLine) {
        this(CSVFormat.DEFAULT, skipFirstLine);
    }

    public ParseCsvWithoutHeader(CSVFormat format, boolean skipFirstLine) {
        this.format = format;
        this.skipFirstLine = skipFirstLine;
    }

    public void apply(Record<String> param, ResultCallback<Map<Integer, String>> callback) {
        try {
            CSVParser csvRecords = this.format.parse(new StringReader(param.getValue()));
            boolean skipped = false;
            for (CSVRecord csvRecord : csvRecords) {
                if (this.skipFirstLine && !skipped) {
                    skipped = true;
                } else {
                    Map<Integer, String> res = new HashMap<>();
                    Iterator<String> iterator = csvRecord.iterator();

                    for (int idx = 0; iterator.hasNext(); ++idx) {
                        res.put(idx, iterator.next());
                    }

                    callback.yield(param.withValue(res));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}