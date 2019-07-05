package job1;

import helpers.CompositeLongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer that calculates the mean trending time and the videos count for each key, which is the video tag.
 * */
public class RankingReducer extends Reducer<Text, TupleWritable, Text, CompositeLongWritable> {

    @Override
    public void reduce(Text key, Iterable<TupleWritable> values, Context context)
            throws IOException, InterruptedException {
        long videosCount = 0;
        long trendingTimeSum = 0;
        for (TupleWritable compositeValue : values) {
            trendingTimeSum += getLongFromTupleWritable(compositeValue, 0);
            videosCount += getLongFromTupleWritable(compositeValue, 1);
        }
        if(values.iterator().hasNext()) {
            context.write(key, new CompositeLongWritable(trendingTimeSum/videosCount, videosCount));
        }
    }

    /**
     * Utility method to get the specified long value from a TupleWritable containing only LongWritables.
     * */
    private long getLongFromTupleWritable(TupleWritable writable, int index){
        return ((LongWritable)writable.get(index)).get();
    }
}
