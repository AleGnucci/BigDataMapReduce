package helpers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RecordSchema {
    public RecordSchema(String message) {
        fields = new ArrayList<FieldDescription>();
        List<String> elements = Arrays.asList(message.split("\n"));
        Iterator<String> it = elements.iterator();
        while(it.hasNext()) {
            String line = it.next().trim().replace(";", "");
            System.err.println("RecordSchema read line: " + line);
            if(line.startsWith("optional") || line.startsWith("required")) {
                String[] parts = line.split(" ");
                FieldDescription field = new FieldDescription();
                field.constraint = parts[0];
                field.type = parts[1];
                field.name = parts[2];
                fields.add(field);
            }
        }
    }
    private List<FieldDescription> fields;
    public List<FieldDescription> getFields() {
        return fields;
    }
}