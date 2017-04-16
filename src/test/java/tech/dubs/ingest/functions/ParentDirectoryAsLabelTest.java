package tech.dubs.ingest.functions;

import org.junit.Test;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.pipelines.PipelineUtils;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class ParentDirectoryAsLabelTest {
    @Test
    public void apply() throws Exception {
        ParentDirectoryAsLabel<String> fn = new ParentDirectoryAsLabel<>();
        Record<String> record = new Record<>("");
        record.putMeta("path", Paths.get("A:/some/path/classname/foo.txt"));
        Record<String> result = PipelineUtils.applyPipelineSingle(fn, record);
        String label = result.getMeta("label", String.class);

        assertEquals("classname", label);
    }

}