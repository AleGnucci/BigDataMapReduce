package job1;

import helpers.CompositeLongWritable;
import org.apache.parquet.example.data.Group;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Mapper that filters out videos with errors and creates for each record as many key-value pairs as the amount of tags
 * inside that record.
 * The key is the tag and the value is the trending time, calculated by computing the difference between dates in days.
 * */
public class RankingMapper extends Mapper<LongWritable, Group, Text, CompositeLongWritable> {

    public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
        if(value.getBoolean("video_error_or_removed", 0)) {
            return;
        }
        Long trendingTime;
        try {
            trendingTime = calculateTrendingTime(value.getString("publish_time", 0),
                    value.getString("trending_date", 0));
        } catch (ParseException exception) {
            System.out.println("launching exception");
            return;
        }
        splitTagsAndWriteOutput(value, context, trendingTime);
    }

    /**
     * Parses the two dates and calculates the difference in days.
     * */
    private Long calculateTrendingTime(String publishTimeString, String trendingTimeString) throws ParseException {
        Date publishTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX").parse(publishTimeString);
        Date trendingDate = new SimpleDateFormat("yy.dd.MM").parse(trendingTimeString);
        return dateDaysDifference(publishTime, trendingDate);
    }

    /**
     * Calculates the date difference in days.
     * */
    private long dateDaysDifference(Date beforeDate, Date afterDate) {
        return TimeUnit.DAYS.convert(afterDate.getTime() - beforeDate.getTime(), TimeUnit.MILLISECONDS);
    }

    /**
     * Removes the quotation marks from the tags and converts them to lowercase,
     * then splits them, removes duplicates and outputs for each tag the composite value.
     * */
    private void splitTagsAndWriteOutput(Group value, Context context, Long trendingTime)
            throws IOException, InterruptedException {
        String[] tags = correctTags(value.getString("tags", 0)).split("\\|");
        List<String> tagList = Arrays.asList(tags);
        Set<String> tagsWithoutDuplicates = new HashSet<>(tagList);
        CompositeLongWritable compositeValue = new CompositeLongWritable(trendingTime, 1);
        for (String tag : tagsWithoutDuplicates) {
            context.write(new Text(tag), compositeValue);
        }
    }

    /**
     * Removes the quotation marks from the tags string. It is the same as this:
     * tags.toLowerCase.replaceAll("\\|\"\"\"", "\\|").replaceAll("\"\"\"\\|", "\\|")
     * .replaceAll("\\|\"\"", "\\|").replaceAll("\"\"\\|", "\\|")
     * .replaceAll("\\|\"", "\\|").replaceAll("\"\\|", "\\|")
     * */
    private String correctTags(String tags) {
        return tags.toLowerCase()
                .replaceAll("[\\|\"\"|\"\"\\||\\|\"|\"\\||\\|\"\"\"|\"\"\"\\|]", "\\|");
    }

}