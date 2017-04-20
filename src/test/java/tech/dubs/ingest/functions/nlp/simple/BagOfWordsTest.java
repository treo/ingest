package tech.dubs.ingest.functions.nlp.simple;

import org.junit.Test;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.functions.nlp.simple.BagOfWords;
import tech.dubs.ingest.pipelines.PipelineUtils;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BagOfWordsTest {
    @Test
    public void apply() throws Exception {
        BagOfWords fn = new BagOfWords();
        Record<Map<String, Integer>> result = PipelineUtils.applyPipelineSingle(fn, new Record<>(Arrays.asList("Hallo", "Welt", "Ich", "habe", "Hallo", "gesagt", "Welt")));
        Map<String, Integer> value = result.getValue();

        assertEquals(value.get("Hallo"), 2, 0);
        assertEquals(value.get("Welt"), 2, 0);
        assertEquals(value.get("Ich"), 1, 0);
        assertEquals(value.get("habe"), 1, 0);
        assertEquals(value.get("gesagt"), 1, 0);
        assertNull(value.get("wurst"));
    }

}