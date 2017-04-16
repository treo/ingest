package tech.dubs.ingest.functions.json;

import org.junit.Test;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.pipelines.PipelineUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ParseJsonTest {
    @Test
    public void apply() throws Exception {
        String json = "{\n" +
                "    \"store\": {\n" +
                "        \"book\": [\n" +
                "            {\n" +
                "                \"category\": \"reference\",\n" +
                "                \"author\": \"Nigel Rees\",\n" +
                "                \"title\": \"Sayings of the Century\",\n" +
                "                \"price\": 8.95\n" +
                "            },\n" +
                "            {\n" +
                "                \"category\": \"fiction\",\n" +
                "                \"author\": \"Evelyn Waugh\",\n" +
                "                \"title\": \"Sword of Honour\",\n" +
                "                \"price\": 12.99\n" +
                "            },\n" +
                "            {\n" +
                "                \"category\": \"fiction\",\n" +
                "                \"author\": \"Herman Melville\",\n" +
                "                \"title\": \"Moby Dick\",\n" +
                "                \"isbn\": \"0-553-21311-3\",\n" +
                "                \"price\": 8.99\n" +
                "            },\n" +
                "            {\n" +
                "                \"category\": \"fiction\",\n" +
                "                \"author\": \"J. R. R. Tolkien\",\n" +
                "                \"title\": \"The Lord of the Rings\",\n" +
                "                \"isbn\": \"0-395-19395-8\",\n" +
                "                \"price\": 22.99\n" +
                "            }\n" +
                "        ],\n" +
                "        \"bicycle\": {\n" +
                "            \"color\": \"red\",\n" +
                "            \"price\": 19.95\n" +
                "        }\n" +
                "    },\n" +
                "    \"expensive\": 10\n" +
                "}";

        ParseJson fn = new ParseJson(
                "category", "$.store.book[*].category",
                "author", "$.store.book[*].author");
        List<Record<Map<String, String>>> records = PipelineUtils.applyPipeline(fn, new Record<>(json));
        Iterator<Map<String, String>> iterator = PipelineUtils.unwrapRecordValues(records.iterator());

        Map<String, String> map = iterator.next();
        assertEquals(map.get("category"), "reference");
        assertEquals(map.get("author"), "Nigel Rees");

        map = iterator.next();
        assertEquals(map.get("category"), "fiction");
        assertEquals(map.get("author"), "Evelyn Waugh");

        map = iterator.next();
        assertEquals(map.get("category"), "fiction");
        assertEquals(map.get("author"), "Herman Melville");

        map = iterator.next();
        assertEquals(map.get("category"), "fiction");
        assertEquals(map.get("author"), "J. R. R. Tolkien");

        assertFalse(iterator.hasNext());
    }

}