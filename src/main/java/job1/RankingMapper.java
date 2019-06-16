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

public class RankingMapper extends Mapper<LongWritable, Group, Text, LongWritable> {
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
