package job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner that sums the trending times and the videos count for each key, which is the video tag.
 * */
public class RankingCombiner extends Reducer<Text, LongWritable, Text, TupleWritable> {
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long videosCount = 0;
        long trendingTimeSum = 0;
        for (LongWritable trendingTime : values) {
            videosCount += 1;
            trendingTimeSum += trendingTime.get();
        }
        context.write(key,
                new TupleWritable(new Writable[]{new LongWritable(trendingTimeSum), new LongWritable(videosCount)}));
    }
}