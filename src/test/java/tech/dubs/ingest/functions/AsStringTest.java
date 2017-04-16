package tech.dubs.ingest.functions;

import org.junit.Test;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.pipelines.PipelineUtils;

import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;

public class AsStringTest {
    @Test
    public void apply() throws Exception {
        AsString fn = new AsString();
        byte[] bytes = "Hellö Wörld".getBytes(Charset.forName("UTF-8"));
        Record<String> result = PipelineUtils.applyPipelineSingle(fn, new Record<>(bytes));
        assertEquals(result.getValue(), "Hellö Wörld");
    }

}