package job1;

import helpers.CompositeLongWritable;

class ReducerLogic {

    private ReducerLogic(){}

    static CompositeLongWritable reduce(Iterable<CompositeLongWritable> values) {
        long videosCount = 0;
        long trendingTimeSum = 0;
        for (CompositeLongWritable compositeValue : values) {
            trendingTimeSum += compositeValue.getFirstValue();
            videosCount += compositeValue.getSecondValue();
        }
        return new CompositeLongWritable(trendingTimeSum, videosCount);
    }

}
