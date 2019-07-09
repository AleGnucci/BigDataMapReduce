package helpers;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OutputSchema {

    private static final List<String> fieldNames = Arrays.asList("tag", "trending_time_avg_days", "videos_count");

    private OutputSchema(){}

    public static Schema getSchema(){
        return new Schema.Parser().parse(
                "{\n" +
                        "  \"type\": \"record\",\n" +
                        "  \"name\": \"Tag_result_info\",\n" +
                        "  \"fields\": [\n" +
                        "    {\"name\": \"" + fieldNames.get(0) + "\", \"type\": \"string\"},\n" +
                        "    {\"name\": \"" + fieldNames.get(1) + "\", \"type\": \"long\"},\n" +
                        "    {\"name\": \"" + fieldNames.get(2) + "\", \"type\": \"long\"}\n" +
                        "  ]\n" +
                        "}");
    }

    public static List<String> getFieldNames(){
        return new ArrayList<>(fieldNames);
    }

}
