package job2;

import helpers.CompositeLongWritable;
import helpers.OutputSchema;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Simple reducer that outputs the input k-v pairs as csv records, with the csv header.
 * */
public class BasicCsvReducer extends Reducer<CompositeLongWritable, Text, Text, Text> {

    /**
     * Used to write to output the csv header.
     * */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String fieldName1 = OutputSchema.getFieldNames().get(0);
        String fieldName2 = OutputSchema.getFieldNames().get(1);
        String fieldName3 = OutputSchema.getFieldNames().get(2);
        context.write(new Text(fieldName1), new Text(fieldName2 + "," + fieldName3)); //header
    }

    @Override
    protected void reduce(CompositeLongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        long firstKeyValue = key.getFirstValue();
        long secondKeyValue = key.getSecondValue();
        for (Text value : values) {
            context.write(value, new Text(firstKeyValue + "," + secondKeyValue));
        }
    }
}
