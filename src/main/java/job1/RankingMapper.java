package job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;

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
public class RankingMapper extends Mapper<LongWritable, Group, Text, LongWritable> {
    public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
        String[] record = value.toString().split("\n")[1].split(": ");

        String[] tags = record[6].split("\\|");
        for (String tag : tags) {
            if(tags[14].equals("False")){
                continue;
            }
            Long trendingTime = calculateTrendingTime(record[5], record[1]);
            if(trendingTime == null) {
                continue;
            }
            context.write(new Text(tag), new LongWritable(trendingTime));
        }
    }

    /**
     * Parses the two dates and calculates the difference in days.
     * */
    private Long calculateTrendingTime(String publishTimeString, String trendingTimeString) {
        Date publishTime;
        Date trendingDate;
        try {
            publishTime = new SimpleDateFormat("yyyy-MM-ddTHH:mm:ss.SSSz").parse(publishTimeString);
            trendingDate = new SimpleDateFormat("yy.dd.MM").parse(trendingTimeString);
        } catch (ParseException e) {
            return null;
        }
        return dateDaysDifference(publishTime, trendingDate);
    }

    /**
     * Calculates the date difference in days.
     * */
    private long dateDaysDifference(Date beforeDate, Date afterDate){
        long millisecondsDifference = Math.abs(afterDate.getTime() - beforeDate.getTime());
        return TimeUnit.DAYS.convert(millisecondsDifference, TimeUnit.MILLISECONDS);
    }
}
