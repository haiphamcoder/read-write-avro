package org.haiphamcoder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.haiphamcoder.adapter.impl.AvroAdapter;
import org.haiphamcoder.adapter.IAvroAdapter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
        Schema schema = new Schema.Parser().parse(new File("src/main/resources/schema/car.avsc"));

        // Create a record
        GenericRecord porsche = new GenericData.Record(schema);
        porsche.put("make", "Porsche");
        porsche.put("model", "911");
        porsche.put("year", 2019);
        porsche.put("horsepower", 443);

        // Create a second record
        GenericRecord bmw = new GenericData.Record(schema);
        bmw.put("make", "BMW");
        bmw.put("model", "840i");
        bmw.put("year", 2020);
        bmw.put("horsepower", 335);

        List<GenericRecord> records = new ArrayList<>();
        Collections.addAll(records, porsche, bmw);

        // Write the records to an Avro file
        IAvroAdapter avroAdapter = new AvroAdapter();
        avroAdapter.writeAvroFile(records, "src/main/resources/data/cars.avro", schema);

        // Read the records from the Avro file
        List<GenericRecord> readRecords = avroAdapter.readAvroFile("src/main/resources/data/cars.avro");
        for (GenericRecord record : readRecords) {
            System.out.println(record.get("make") + " " + record.get("model"));
        }
    }
}