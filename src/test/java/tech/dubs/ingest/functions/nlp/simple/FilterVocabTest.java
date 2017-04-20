package tech.dubs.ingest.functions.nlp.simple;

import org.junit.Test;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.functions.nlp.simple.FilterVocab;
import tech.dubs.ingest.pipelines.PipelineUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilterVocabTest {
    @Test
    public void apply() throws Exception {
        FilterVocab fn = new FilterVocab(new HashSet<>(Arrays.asList("bad1", "bad2")), false);
        Record<List<String>> result = PipelineUtils.applyPipelineSingle(fn, new Record<>(Arrays.asList("This", "is", "not", "a", "bad1", "good", "bad2", "string")));
        List<String> value = result.getValue();

        assertTrue(value.containsAll(Arrays.asList("This", "is", "not", "a", "good", "string")));
        assertFalse(value.contains("bad1"));
        assertFalse(value.contains("bad2"));
    }

}