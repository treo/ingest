package tech.dubs.ingest.functions.csv;

import org.junit.Test;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.pipelines.PipelineUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ParseCsvWithHeaderTest {
    @Test
    public void apply() throws Exception {
        ParseCsvWithHeader fn = new ParseCsvWithHeader();
        String csv = "id,name,value\n7,foo,bar\n18,spam,eggs";
        List<Record<Map<String, String>>> record = PipelineUtils.applyPipeline(fn, new Record<String>(csv));
        Iterator<Map<String, String>> maps = PipelineUtils.unwrapRecordValues(record.iterator());
        Map<String, String> map = maps.next();

        assertEquals(map.get("id"), "7");
        assertEquals(map.get("name"), "foo");
        assertEquals(map.get("value"), "bar");

        map = maps.next();

        assertEquals(map.get("id"), "18");
        assertEquals(map.get("name"), "spam");
        assertEquals(map.get("value"), "eggs");

        assertFalse(maps.hasNext());
    }

}