package job1;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.parquet.example.data.Group;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Mapper that filters out videos with errors and creates for each record as many key-value pairs as the amount of tags
 * inside that record.
 * The key is the tag and the value is the trending time, calculated by computing the difference between dates in days.
 * */
public class RankingMapper extends Mapper<LongWritable, Group, Text, TupleWritable> { //TODO: usa GenericTwoLongWritable al posto di TupleWritable in output

    public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
        String[] tags = correctTags(value.getString("tags", 0)).split("\\|");
        for (String tag : tags) {
            boolean videoHasErrors = value.getString("video_error_or_removed", 0)
                    .toLowerCase().equals("true");
            if(videoHasErrors){
                continue;
            }
            Long trendingTime = calculateTrendingTime(value.getString("publish_time", 0),
                    value.getString("trending_date", 0));
            if(trendingTime == null) {
                continue;
            }
            context.write(new Text(tag), new TupleWritable(new Writable[]{new LongWritable(trendingTime)}));
        }
    }

    private String correctTags(String tags) {
        return tags.replaceAll("\\|\"\"", "\\|\"").replaceAll("\"\"\\|", "\"\\|");
    }

    /**
     * Parses the two dates and calculates the difference in days.
     * */
    private Long calculateTrendingTime(String publishTimeString, String trendingTimeString) {
        Date publishTime;
        Date trendingDate;
        try {
            publishTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX").parse(publishTimeString);
            trendingDate = new SimpleDateFormat("yy.dd.MM").parse(trendingTimeString);
        } catch (ParseException e) {
            return null;
        }
        return dateDaysDifference(publishTime, trendingDate);
    }

    /**
     * Calculates the date difference in days.
     * */
    private long dateDaysDifference(Date beforeDate, Date afterDate) {
        long millisecondsDifference = Math.abs(afterDate.getTime() - beforeDate.getTime());
        return TimeUnit.DAYS.convert(millisecondsDifference, TimeUnit.MILLISECONDS);
    }
}