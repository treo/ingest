package tech.dubs.ingest.functions.nlp;

import org.junit.Test;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.pipelines.PipelineUtils;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class SimpleTokenizerTest {
    @Test
    public void apply() throws Exception {
        SimpleTokenizer fn = new SimpleTokenizer();
        Record<List<String>> result = PipelineUtils.applyPipelineSingle(fn, new Record<>("Hello World, this is a tokenizer test"));
        List<String> value = result.getValue();

        assertTrue(value.containsAll(Arrays.asList("Hello", "World", "this", "is", "a", "tokenizer", "test")));
    }

}