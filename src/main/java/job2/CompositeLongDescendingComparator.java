package job2;

import helpers.CompositeLong;
import helpers.CompositeLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Comparator class, used to compare objects of type CompositeLong.
 * */
public class CompositeLongDescendingComparator extends WritableComparator {

    public CompositeLongDescendingComparator() {
        super(CompositeLongWritable.class, true);
    }

    @Override
    public int compare(WritableComparable writable1, WritableComparable writable2) {
        CompositeLong compositeValue1 = (CompositeLong) writable1;
        CompositeLong compositeValue2 = (CompositeLong) writable2;
        //uses the comparison logic inside the CompositeLong class, but inverts the output sign, to have descending order
        return -compositeValue1.compareTo(compositeValue2);
    }
}
