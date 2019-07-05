package job2;

import helpers.CompositeLongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BasicReducer extends Reducer<CompositeLongWritable, Text, CompositeLongWritable, Text> {

    @Override
    protected void reduce(CompositeLongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key, value);
        }
    }
}
