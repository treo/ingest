package tech.dubs.ingest.functions;

import org.junit.Test;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.pipelines.PipelineUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;

public class EncodeOneHotTest {
    @Test
    public void apply() throws Exception {
        HashMap<String, List<Object>> categories = new HashMap<>();
        categories.put("A", Arrays.<Object>asList("a", "b", "c"));
        categories.put("#", Arrays.<Object>asList(1, 2, 3));
        EncodeOneHot<String> fn = new EncodeOneHot<>(categories, "A", "#");

        Map<String, Object> input = new HashMap<>();
        input.put("A", "b");
        input.put("#", 3);
        Record<Map<String, Object>> result = PipelineUtils.applyPipelineSingle(fn, new Record<>(input));
        Map<String, Object> value = result.getValue();
        double[] aVal = (double[]) value.get("A");
        assertArrayEquals(aVal, new double[]{0, 1, 0}, 0);
        double[] numVal = (double[]) value.get("#");
        assertArrayEquals(numVal, new double[]{0, 0, 1}, 0);
    }

}