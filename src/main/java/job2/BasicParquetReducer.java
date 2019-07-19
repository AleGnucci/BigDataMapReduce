package job2;

import helpers.CompositeLongWritable;
import helpers.OutputSchema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Simple reducer that outputs the input k-v pairs as parquet records.
 * */
public class BasicParquetReducer extends Reducer<CompositeLongWritable, Text, Void, GenericRecord> {

    private GenericRecord record = new GenericData.Record(OutputSchema.getSchema());
    private String fieldName1 = OutputSchema.getFieldNames().get(0);
    private String fieldName2 = OutputSchema.getFieldNames().get(1);
    private String fieldName3 = OutputSchema.getFieldNames().get(2);

    @Override
    protected void reduce(CompositeLongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long firstKeyValue = key.getFirstValue();
        long secondKeyValue = key.getSecondValue();
        for (Text value : values) {
            record.put(fieldName1, value.toString());
            record.put(fieldName2, firstKeyValue);
            record.put(fieldName3, secondKeyValue);
            context.write(null, record);
        }
    }
}
