package tech.dubs.ingest.functions.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

public class ParseCsvWithHeader implements Function<String, Map<String, String>> {
    private final CSVFormat format;

    public ParseCsvWithHeader() {
        this(CSVFormat.DEFAULT.withFirstRecordAsHeader());
    }

    public ParseCsvWithHeader(CSVFormat format) {
        this.format = format;
    }

    public void apply(Record<String> param, ResultCallback<Map<String, String>> callback) {
        try {
            CSVParser csvRecords = this.format.parse(new StringReader(param.getValue()));
            for (CSVRecord csvRecord : csvRecords) {
                Map<String, String> map = csvRecord.toMap();
                callback.yield(param.withValue(map));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
