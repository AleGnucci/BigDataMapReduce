package helpers;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.api.DelegatingReadSupport;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.example.GroupReadSupport;

public class ParquetReadSupport extends DelegatingReadSupport<Group> {

    public ParquetReadSupport() {
        super(new GroupReadSupport());
    }

    @Override
    public org.apache.parquet.hadoop.api.ReadSupport.ReadContext init(InitContext context) {
        return super.init(context);
    }
}