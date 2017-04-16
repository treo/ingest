package tech.dubs.ingest.util;

import org.junit.Test;
import tech.dubs.ingest.api.Record;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class CallbackValueTest {
    @Test
    public void yield() throws Exception {
        CallbackValue<Integer> value = new CallbackValue<>();
        value.yield(new Record<>(17));
        assertEquals(17, value.getResult().getValue(), 0);
    }

    @Test
    public void getResults() throws Exception {
        CallbackValue<Integer> value = new CallbackValue<>();
        value.yield(new Record<>(1));
        value.yield(new Record<>(2));
        value.yield(new Record<>(3));
        List<Record<Integer>> results = value.getResults();
        assertEquals(results.size(), 3);
    }
}