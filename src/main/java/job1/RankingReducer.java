package job1;

import helpers.CompositeLongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Reducer that calculates the mean trending time and the videos count for each key, which is the video tag.
 * */
public class RankingReducer extends Reducer<Text, CompositeLongWritable, Text, CompositeLongWritable> {

    @Override
    public void reduce(Text key, Iterable<CompositeLongWritable> values, Context context)
            throws IOException, InterruptedException {
        CompositeLongWritable reducedInput = ReducerLogic.reduce(values);
        context.write(key, new CompositeLongWritable(reducedInput.getFirstValue()/reducedInput.getSecondValue(),
                reducedInput.getSecondValue()));
    }
}
