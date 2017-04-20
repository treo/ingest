package tech.dubs.ingest.functions;

import org.junit.Test;
import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.functions.generic.ApplyToKeys;
import tech.dubs.ingest.pipelines.PipelineUtils;
import tech.dubs.ingest.pipelines.SerialPipeline;

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
        Function<Map<String, Object>, Map<String, Object>> fn = new SerialPipeline<Map<String, Object>, Map<String, Object>>()
                .add(new ApplyToKeys<>(new EncodeOneHot<>(categories.get("A")), "A"))
                .add(new ApplyToKeys<>(new EncodeOneHot<>(categories.get("#")), "#"));


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