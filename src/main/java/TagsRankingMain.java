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

    /**
     * This is the main job and the first to be executed: it calculates the required result, but does not sort it.
    * */
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

    /**
     * This job gets executed after the first one and it just sorts the results using the videos count.
     * */
    private static void job2(Configuration conf, String[] args) throws Exception {
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job2 = Job.getInstance(conf, "sorting job");
        job2.setJarByClass(TagsRankingMain.class);
        job2.setMapperClass(KeyValueSwappingMapper.class); //mapper that swaps keys with values
        job2.setNumReduceTasks(1); //sets only one reducer, so there is only one output file
        //sorts the key-value pairs before they arrive to the reducer
        job2.setSortComparatorClass(CompositeLongComparator.class);
        job2.setOutputKeyClass(CompositeLongWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(outputPath, "out1"));
        FileOutputFormat.setOutputPath(job2, outputPath);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}