package org.haiphamcoder.adapter.impl;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.haiphamcoder.adapter.IAvroAdapter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class AvroAdapter implements IAvroAdapter {
    @Override
    public void writeAvroFile(List<GenericRecord> records, String fileUri, Schema schema) {
        File file = new File(fileUri);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        try {
            dataFileWriter.create(schema, file);
            for (GenericRecord record : records) {
                dataFileWriter.append(record);
            }
            dataFileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<GenericRecord> readAvroFile(String fileUri) {
        List<GenericRecord> records = new ArrayList<>();

        try {
            File inputFile = new File(fileUri);
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(inputFile, datumReader);
            while (dataFileReader.hasNext()) {
                records.add(dataFileReader.next());
            }
            dataFileReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return records;
    }
}
