package tech.dubs.ingest.functions.html;

import org.junit.Test;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.pipelines.PipelineUtils;

import static org.junit.Assert.assertEquals;

public class HtmlToTextTest {
    @Test
    public void apply() throws Exception {
        String code = "<p>I would like to publish part <i>or</i> full source code of one or more plugins (licensed under " +
                "GPL, GPLv2, MIT or no license) in my website/blog. Website I am talking about is for everyone, free" +
                " to use and free to copy code. Plus, <b>is it okay</b> if I do not provide any link to the source " +
                "code/plugin? I will <pre>definitely give full credits</pre> to the developer.</p>\n";
        String target = "I would like to publish part or full source code of one or more plugins (licensed under " +
                "GPL, GPLv2, MIT or no license) in my website/blog. Website I am talking about is for everyone, free" +
                " to use and free to copy code. Plus, is it okay if I do not provide any link to the source " +
                "code/plugin? I will definitely give full credits to the developer.";
        HtmlToText fn = new HtmlToText();
        Record<String> result = PipelineUtils.applyPipelineSingle(fn, new Record<String>(code));
        String value = result.getValue();


        assertEquals(target, value);
    }

}