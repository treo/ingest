package tech.dubs.ingest.functions.nd4j;

import org.junit.Test;
import org.nd4j.linalg.dataset.DataSet;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.pipelines.PipelineUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TransformToDataSetTest {
    @Test
    public void apply() throws Exception {
        TransformToDataSet<String> fn = new TransformToDataSet<>(Arrays.asList("height", "width"), Collections.singletonList("class"));
        Map<String, Object> values = new HashMap<>();
        values.put("height", 7d);
        values.put("width", 8d);
        values.put("class", 12d);
        Record<Map<String, Object>> record = new Record<>(values);
        Record<DataSet> dataSetRecord = PipelineUtils.applyPipelineSingle(fn, record);
        DataSet value = dataSetRecord.getValue();

        assertArrayEquals(value.getFeatureMatrix().shape(), new int[]{1, 2});
        assertArrayEquals(value.getLabels().shape(), new int[]{1, 1});

        assertEquals(value.getFeatureMatrix().getDouble(0,0), 7, 0);
        assertEquals(value.getFeatureMatrix().getDouble(0,1), 8, 0);
        assertEquals(value.getLabels().getDouble(0,0), 12, 0);
    }

}