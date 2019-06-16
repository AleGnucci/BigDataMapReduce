package helpers;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeLongWritable implements WritableComparable<CompositeLong>, CompositeLong {
    private long value1 = 0;
    private long value2 = 0; //comparisons on objects of this class are done considering only this field

    public CompositeLongWritable() {}

    public CompositeLongWritable(long value1, long value2) {
        this.value1 = value1;
        this.value2 = value2;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value1 = in.readLong();
        value2 = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(value1);
        out.writeLong(value2);
    }

    public void merge(CompositeLongWritable other) {
        this.value1 += other.value1;
        this.value2 += other.value2;
    }

    @Override
    public String toString() {
        return this.value1 + "\t" + this.value2 + "\t";
    }

    @Override
    public int compareTo(CompositeLong compositeLong) {
        return (Long.compare(value2, compositeLong.getSecondValue()));
    }

    @Override
    public long getFirstValue() {
        return value1;
    }

    @Override
    public long getSecondValue() {
        return value2;
    }
}