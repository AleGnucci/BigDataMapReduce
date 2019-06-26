package helpers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.hadoop.ParquetInputSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetReader {

    private static ParquetReader instance = null;

    private ParquetReader() { }

    public static ParquetReader getInstance() {
        if (instance == null) {
            instance = new ParquetReader();
        }
        return instance;
    }

    public List<String> readParquetRecord(Text value, Mapper.Context context, List<FieldDescription> expectedFields)
            throws IOException {

        if(expectedFields == null) {
            // Get the file schema which may be different from the fields in a particular record) from the input split
            //String fileSchema = ((ParquetInputSplit)context.getInputSplit()).getFileSchema(); //TODO: org.apache.hadoop.mapreduce.lib.input.FileSplit cannot be cast to ParquetInputSplit
            String fileSchema = convertToParquetFileSplit(
                    (org.apache.hadoop.mapreduce.lib.input.FileSplit)context.getInputSplit()).getFileSchema(); //TODO: splits no longer have the file schema, see PARQUET-234
            // System.err.println("file schema from context: " + fileSchema);
            RecordSchema schema = new RecordSchema(fileSchema);
            expectedFields = schema.getFields();
            //System.err.println("inferred schema: " + expectedFields.toString());
        }

        // No public accessor to the column values in a Group, so extract them from the string representation
        String line = value.toString();
        String[] fields = line.split("\n");

        List<String> record = new ArrayList<>();
        int i = 0;
        // Look for each expected column
        for (FieldDescription expectedField : expectedFields) {
            String name = expectedField.name;
            if (fields.length > i) {
                String[] parts = fields[i].split(": ");
                // We assume proper order, but there may be fields missing
                if (parts[0].equals(name)) {
                    record.add(parts[1]);
                    i++;
                }
            }
        }
        return record;
    }

    private static ParquetInputSplit convertToParquetFileSplit(FileSplit split) throws IOException {
        return new ParquetInputSplit(split.getPath(),
                split.getStart(), split.getStart() + split.getLength(),
                split.getLength(), split.getLocations(), null);
    }
}
