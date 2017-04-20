package tech.dubs.ingest.functions;

import org.junit.Test;
import tech.dubs.ingest.api.Pipeline;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.functions.csv.ParseCsvWithHeader;
import tech.dubs.ingest.functions.generic.ApplyToKeys;
import tech.dubs.ingest.pipelines.PipelineUtils;
import tech.dubs.ingest.pipelines.SerialPipeline;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;

public class ParseCategoricalTest {
    @Test
    public void collectCategories() throws Exception {
        ParseCsvWithHeader fn = new ParseCsvWithHeader();
        String csv = "id,name,value\n7,foo,bar\n18,spam,eggs\n9,a,bar";
        List<Record<Map<String, String>>> record = PipelineUtils.applyPipeline(fn, new Record<>(csv));

        Map<String, List<Object>> categories = ParseCategorical.collectCategories((Iterator)record.iterator(), "value");
        assertEquals(categories.get("value"), Arrays.asList("bar", "eggs"));
    }

    @Test
    public void apply() throws Exception {
        ParseCsvWithHeader fn = new ParseCsvWithHeader();
        String csv = "id,name,value\n7,foo,bar\n18,spam,eggs\n9,a,bar";
        List<Record<Map<String, String>>> record = PipelineUtils.applyPipeline(fn, new Record<String>(csv));
        Map<String, List<String>> categories = ParseCategorical.collectCategories((Iterator)record.iterator(), "value");

        Pipeline<String, Map<String, Object>> pipeline = new SerialPipeline<String, String>()
                .add(new ParseCsvWithHeader())
                .add(new ApplyToKeys<>(new ParseCategorical<>(categories.get("value")), "value"));

        List<Record<Map<String, Object>>> records = PipelineUtils.applyPipeline(pipeline, new Record<>(csv));
        Iterator<Map<String, Object>> maps = PipelineUtils.unwrapRecordValues(records.iterator());

        Map<String, Object> map = maps.next();
        assertEquals(map.get("value"), 0);
        map = maps.next();
        assertEquals(map.get("value"), 1);
        map = maps.next();
        assertEquals(map.get("value"), 0);

        assertFalse(maps.hasNext());

    }

}