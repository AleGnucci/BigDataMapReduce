package job1;

import helpers.CompositeLongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RankingReducer extends Reducer<Text, TupleWritable, Text, CompositeLongWritable> {

    public void reduce(Text key, Iterable<TupleWritable> values, Context context)
            throws IOException, InterruptedException {
        long videosCount = 0;
        long trendingTimeSum = 0;
        for (TupleWritable compositeValue : values) {
            trendingTimeSum += getLongFromTupleWritable(compositeValue, 0);
            videosCount += getLongFromTupleWritable(compositeValue, 1);
        }
        context.write(key, new CompositeLongWritable(trendingTimeSum/videosCount, videosCount));
    }

    private long getLongFromTupleWritable(TupleWritable writable, int index){
        return ((LongWritable)writable.get(index)).get();
    }
}
