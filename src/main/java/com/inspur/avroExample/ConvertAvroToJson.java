package com.inspur.avroExample;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// 参考 https://www.cnblogs.com/sco1234/p/7833345.html
public class ConvertAvroToJson {

    // https://github.com/nnsantosh/AvroSerializeDeserialze/blob/master/src/main/java/com/test/avro/samples/ReadAvroFile.java
    public static void main(String[] args) {

        Schema userSchema = null;
        DataFileReader<GenericRecord> dataFileReader = null;
        try {
            userSchema = new Schema.Parser().parse(new File("src/main/avro/user.avsc"));
            String outputPath = "src/main/avro/users.avro";
            File inputAvroFile = new File(outputPath);

            DatumReader<GenericRecord> datumReader = new SpecificDatumReader<GenericRecord>();
            dataFileReader = new DataFileReader<GenericRecord>(inputAvroFile, datumReader);

            List<GenericRecord> records = new ArrayList<GenericRecord>();

            while (dataFileReader.hasNext()) {

                // Reuse user object by passing it to next(). This saves us from
                // allocating and garbage collecting many objects for files with
                // many items.
                records.add(dataFileReader.next());

            }
            System.out.println(records);

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            try {
                dataFileReader.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }
}
