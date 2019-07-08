package helpers;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Writable that contains two long fields. Comparisons on objects of this type are done using only the second field.
 * */
public class CompositeLongWritable implements WritableComparable<CompositeLong>, CompositeLong {
    private long value1 = 0;
    private long value2 = 0; //to sort objects of this type only this field is used

    public CompositeLongWritable(){}

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

    @Override
    public boolean equals(Object otherObject) {
        if (this == otherObject) return true;
        if (otherObject == null || getClass() != otherObject.getClass()) return false;
        CompositeLongWritable that = (CompositeLongWritable) otherObject;
        return value1 == that.value1 && value2 == that.value2;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value1, value2);
    }
}