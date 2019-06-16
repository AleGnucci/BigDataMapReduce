import helpers.CompositeLongWritable;
import job1.RankingCombiner;
import job1.RankingMapper;
import job1.RankingReducer;
import job2.CompositeLongComparator;
import job2.KeyValueSwappingMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class TagsRankingMain {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        job1(conf, args);
        job2(conf, args);
    }

    private static void job1(Configuration conf, String[] args) throws Exception{
        Job job = Job.getInstance(conf, "job that calculates the required information about the tags");

        Path inputPath = new Path(args[0]), outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job.setJarByClass(TagsRankingMain.class);
        job.setMapperClass(RankingMapper.class);
        job.setCombinerClass(RankingCombiner.class);

        if(args.length>2){
            if(Integer.parseInt(args[2])>=0){
                job.setNumReduceTasks(Integer.parseInt(args[2]));
            }
        }
        job.setReducerClass(RankingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CompositeLongWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath, "out1"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void job2(Configuration conf, String[] args) throws Exception {
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job2 = Job.getInstance(conf, "sorting job");
        job2.setJarByClass(TagsRankingMain.class);
        job2.setMapperClass(KeyValueSwappingMapper.class);
        job2.setNumReduceTasks(1);
        job2.setSortComparatorClass(CompositeLongComparator.class);
        job2.setOutputKeyClass(CompositeLongWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(outputPath, "out1"));
        FileOutputFormat.setOutputPath(job2, outputPath);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}