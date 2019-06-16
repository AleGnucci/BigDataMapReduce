import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.parquet.example.data.Group;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TagsRanking {

    public static class RankingMapper extends Mapper<LongWritable, Group, Text, LongWritable> {
        public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
            String[] record = value.toString().split("\n")[1].split(": ");

            String[] tags = record[6].split("\\|");
            for (String tag : tags) {
                if(tags[14].equals("False")){
                    continue;
                }
                Date publish_time;
                Date trending_date;
                try {
                    publish_time = new SimpleDateFormat("yyyyy-MM-ddTHH:mm:ss.SSSz").parse(record[5]);
                    trending_date = new SimpleDateFormat("yy.dd.MM").parse(record[1]);
                } catch (ParseException e) {
                    continue;
                }
                long trendingTime = dateDaysDifference(publish_time, trending_date);
                context.write(new Text(tag), new LongWritable(trendingTime));
            }
        }

        private long dateDaysDifference(Date beforeDate, Date afterDate){
            long millisecondsDifference = Math.abs(afterDate.getTime() - beforeDate.getTime());
            return TimeUnit.DAYS.convert(millisecondsDifference, TimeUnit.MILLISECONDS);
        }
    }

    public static class RankingCombiner extends Reducer<Text, LongWritable, Text, TupleWritable> {
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

    public static class RankingReducer extends Reducer<Text, TupleWritable, Text, TupleWritable> {

        public void reduce(Text key, Iterable<TupleWritable> values, Context context)
                throws IOException, InterruptedException {
            long videosCount = 0;
            long trendingTimeSum = 0;
            for (TupleWritable trendingTime : values) {
                trendingTimeSum += ((LongWritable)trendingTime.get(0)).get();
                videosCount += ((LongWritable)trendingTime.get(1)).get();
            }
            LongWritable trendingTimeAverage = new LongWritable(trendingTimeSum/videosCount);
            context.write(key, new TupleWritable(new Writable[]{trendingTimeAverage, new LongWritable(videosCount)}));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "etl phase");

        Path inputPath = new Path(args[0]), outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job.setJarByClass(TagsRanking.class);
        job.setMapperClass(RankingMapper.class);
        job.setCombinerClass(RankingCombiner.class);

        if(args.length>2){
            if(Integer.parseInt(args[2])>=0){
                job.setNumReduceTasks(Integer.parseInt(args[2]));
            }
        }
        job.setReducerClass(RankingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}