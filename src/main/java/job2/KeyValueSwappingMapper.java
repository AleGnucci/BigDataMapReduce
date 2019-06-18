package job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Simple mapper that swaps keys with values and viceversa.
 * */
public class KeyValueSwappingMapper extends Mapper<Text, LongWritable, LongWritable, Text> {

    public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        context.write(value, key);
    }
}