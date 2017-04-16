package tech.dubs.ingest.functions.json;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import tech.dubs.ingest.api.Function;
import tech.dubs.ingest.api.Record;
import tech.dubs.ingest.api.ResultCallback;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParseJson implements Function<String, Map<String, String>> {

    private final Map<String, String> columnNameToJsonPath;
    private final Configuration configuration;

    public ParseJson(String... namePathPairs) {
        if(namePathPairs.length % 2 != 0){
            throw new IllegalArgumentException("You have to provide the parameters as pairs of key, path Strings");
        }

        columnNameToJsonPath = new HashMap<>();
        for (int i = 0; i < namePathPairs.length; i+=2) {
            columnNameToJsonPath.put(namePathPairs[i], namePathPairs[i+1]);
        }

        configuration = Configuration.defaultConfiguration();
        configuration.addOptions(Option.ALWAYS_RETURN_LIST);
    }

    @Override
    public void apply(Record<String> input, ResultCallback<Map<String, String>> callback) {
        DocumentContext context = JsonPath.using(configuration).parse(input.getValue());
        List<Map<String, String>> results = new ArrayList<>();
        boolean firstKey = true;
        int firstSize = 0;
        for (Map.Entry<String, String> entry : columnNameToJsonPath.entrySet()) {
            List<String> values = context.read(entry.getValue());
            if(firstKey)
                firstSize = values.size();
            else if (firstSize != values.size()){
                throw new IllegalArgumentException("The json paths you provided do not result in equally long results!");
            }
            for (int i = 0; i < values.size(); i++) {
                Map<String, String> map;
                String value = values.get(i);
                if(firstKey){
                    map = new HashMap<>();
                    results.add(map);
                }else{
                    map = results.get(i);
                }
                map.put(entry.getKey(), value);
            }
            firstKey = false;
        }
        for (Map<String, String> result : results) {
            callback.yield(input.withValue(result));
        }
    }
}
