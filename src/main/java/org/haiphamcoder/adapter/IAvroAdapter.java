package org.haiphamcoder.adapter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

public interface IAvroAdapter {
    void writeAvroFile(List<GenericRecord> records, String fileUri, Schema schema);

    List<GenericRecord> readAvroFile(String fileUri);
}
