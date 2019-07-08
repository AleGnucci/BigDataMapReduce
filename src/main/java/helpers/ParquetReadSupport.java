package helpers;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.api.DelegatingReadSupport;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.example.GroupReadSupport;

/**
 * Class needed to read Parquet files in mapreduce.
 * */
public class ParquetReadSupport extends DelegatingReadSupport<Group> {

    public ParquetReadSupport() {
        super(new GroupReadSupport());
    }

    @Override
    public org.apache.parquet.hadoop.api.ReadSupport.ReadContext init(InitContext context) {
        return super.init(context);
    }
}