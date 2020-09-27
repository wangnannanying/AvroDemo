package com.inspur.avroExample;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

// 参考 https://github.com/shivaganesh2232/Json-to-Avro-Csv/blob/master/Json-to-Avro-Csv/src/Fromjson.java
public class ConvertJsonToAvro {

    public static void main(String[] args) throws IOException {

        InputStream myJSONInputStream = null ;
        DataFileWriter<GenericRecord> dataFileWriter = null;
        try {

            // Step 1: Read the avro schema
            Schema schema = new Schema.Parser().parse(new File("src/main/resources/products.avsc"));

            // Step 2: Build the input stream out of the JSON payload
            myJSONInputStream = new FileInputStream("src/main/resources/products.json");
            byte[] myJSONBytes = IOUtils.toByteArray(myJSONInputStream);

            //Step 3: Give the location for AVRO file generation
            File file = new File("src/main/resources/products.avro");

            //Step 4: Create the AVRO file writer with the right schema path and output path
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
            dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
            dataFileWriter.create(schema, file);

            //Step 5: Write the data in input byte stream
            System.out.println("Started creating Avro file - Serialization Initiated....");

            ByteBuffer datum = ByteBuffer.wrap(myJSONBytes);
            dataFileWriter.appendEncoded(datum);

            System.out.println("Successfully created Avro file - Serialization Successful.");
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            myJSONInputStream.close();
            dataFileWriter.close();
        }

    }

}
