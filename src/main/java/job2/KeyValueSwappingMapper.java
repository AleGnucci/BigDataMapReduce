package job2;

import helpers.CompositeLongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Simple mapper that swaps keys with values and vice versa.
 * */
public class KeyValueSwappingMapper extends Mapper<Text, CompositeLongWritable, CompositeLongWritable, Text> {

    public void map(Text key, CompositeLongWritable value, Context context) throws IOException, InterruptedException {
        context.write(value, key);
    }
}