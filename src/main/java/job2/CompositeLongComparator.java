package job2;

import helpers.CompositeLong;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeLongComparator extends WritableComparator {

    @Override
    public int compare(WritableComparable writable1, WritableComparable writable2) {
        CompositeLong compositeValue1 = (CompositeLong) writable1;
        CompositeLong compositeValue2 = (CompositeLong) writable2;
        return compositeValue1.compareTo(compositeValue2);
    }
}
