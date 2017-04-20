package tech.dubs.ingest.functions.nlp.simple;

import org.junit.Test;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.functions.nlp.simple.ReplaceMatching;
import tech.dubs.ingest.pipelines.PipelineUtils;

import static org.junit.Assert.assertEquals;

public class ReplaceMatchingTest {
    @Test
    public void apply() throws Exception {
        ReplaceMatching fn = new ReplaceMatching("``.*?''", "");
        String input = "This is a ``stupid'' way to mark inline code";

        Record<String> result = PipelineUtils.applyPipelineSingle(fn, new Record<String>(input));
        assertEquals("This is a  way to mark inline code", result.getValue());
    }

}