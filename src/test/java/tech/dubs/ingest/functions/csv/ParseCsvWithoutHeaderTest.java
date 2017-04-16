package tech.dubs.ingest.functions.csv;

import org.junit.Test;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.pipelines.PipelineUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ParseCsvWithoutHeaderTest {
    @Test
    public void apply() throws Exception {
        ParseCsvWithoutHeader fn = new ParseCsvWithoutHeader(true);
        String csv = "id,name,value\n7,foo,bar\n18,spam,eggs";
        List<Record<Map<Integer, String>>> record = PipelineUtils.applyPipeline(fn, new Record<String>(csv));
        Iterator<Map<Integer, String>> maps = PipelineUtils.unwrapRecordValues(record.iterator());
        Map<Integer, String> map = maps.next();

        assertEquals(map.get(0), "7");
        assertEquals(map.get(1), "foo");
        assertEquals(map.get(2), "bar");

        map = maps.next();

        assertEquals(map.get(0), "18");
        assertEquals(map.get(1), "spam");
        assertEquals(map.get(2), "eggs");

        assertFalse(maps.hasNext());
    }

}