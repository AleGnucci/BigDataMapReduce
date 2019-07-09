package job2;

import helpers.CompositeLongWritable;
import helpers.OutputSchema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

public class BasicParquetReducer extends Reducer<CompositeLongWritable, Text, Void, GenericRecord> {

    private GenericRecord record = new GenericData.Record(OutputSchema.getSchema());

    @Override
    protected void reduce(CompositeLongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        List<String> fieldNames = OutputSchema.getFieldNames();
        for (Text value : values) {
            record.put(fieldNames.get(0), value.toString());
            record.put(fieldNames.get(1), key.getFirstValue());
            record.put(fieldNames.get(2), key.getSecondValue());
            context.write(null, record);
        }
    }
}
