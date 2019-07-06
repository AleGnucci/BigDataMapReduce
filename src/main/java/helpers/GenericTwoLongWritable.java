package helpers;

import org.apache.hadoop.io.GenericWritable;

public class GenericTwoLongWritable extends GenericWritable { //TODO: fallo come CompositelongWritable, ma implementando Writable

    private static Class[] CLASSES = {
            Long.class,
            Long.class
    };

    protected Class[] getTypes() {
        return CLASSES;
    }

}
