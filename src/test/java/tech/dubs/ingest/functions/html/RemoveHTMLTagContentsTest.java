package tech.dubs.ingest.functions.html;

import org.junit.Test;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.pipelines.PipelineUtils;

import static org.junit.Assert.assertEquals;

public class RemoveHTMLTagContentsTest {
    @Test
    public void apply() throws Exception {
        String code = "<p>This is one of the best algorithms to calculate the nth Fibonacci sequence. it needs O(log(n)) time to do its job, so it's so efficient. I found it somewhere but don't know how it works!\n" +
                "Can anyone tell me how this algorithm works? thanks.\n" +
                "Here's the code:</p>\n" +
                "\n" +
                "<pre><code>int fib3 (int n) {\n" +
                "\n" +
                "    int i = 1, j = 0, k = 0, h = 1, t;\n" +
                "    while (n &gt; 0) {\n" +
                "        if (n % 2) {\n" +
                "            t = j * h;\n" +
                "            j = i * h + j * k + t;\n" +
                "            i = i * k + t;\n" +
                "        }\n" +
                "        t = h * h;\n" +
                "        h = 2 * k * h + t;\n" +
                "        k = k * k + t;\n" +
                "        n /= 2;\n" +
                "    }\n" +
                "    return j;\n" +
                "}\n" +
                "</code></pre>\n";

        String target =  "<p>This is one of the best algorithms to calculate the nth Fibonacci sequence. it needs O(log(n)) time to do its job, so it's so efficient. I found it somewhere but don't know how it works! " +
                "Can anyone tell me how this algorithm works? thanks. " +
                "Here's the code:</p>";
        RemoveHTMLTagContents fn = new RemoveHTMLTagContents("pre", "code");
        Record<String> result = PipelineUtils.applyPipelineSingle(fn, new Record<String>(code));
        String value = result.getValue();

        assertEquals(target, value);
    }

}