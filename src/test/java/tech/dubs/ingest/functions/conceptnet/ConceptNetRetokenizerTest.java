package tech.dubs.ingest.functions.conceptnet;

import org.junit.Ignore;
import org.junit.Test;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.pipelines.PipelineUtils;

import java.util.Arrays;
import java.util.List;

@Ignore
public class ConceptNetRetokenizerTest {
    @Test
    public void apply() throws Exception {

        ConceptNetRetokenizer retokenizer = ConceptNetRetokenizer.load("C:\\Users\\dubs\\Documents\\Studium\\Master Arbeit\\numberbatch-17.02.txt");

        Record<List<String>> record = new Record<>(Arrays.asList("we", "are", "looking", "for", "a", "software", "developer"));
        Record<List<Integer>> result = PipelineUtils.applyPipelineSingle(retokenizer, record);

    }

}