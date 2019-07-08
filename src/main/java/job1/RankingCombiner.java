package job1;

import helpers.CompositeLongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner that sums the trending times and the videos count for each key, which is the video tag.
 * */
public class RankingCombiner extends Reducer<Text, CompositeLongWritable, Text, CompositeLongWritable> {
    public void reduce(Text key, Iterable<CompositeLongWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, ReducerLogic.reduce(values));
    }
}