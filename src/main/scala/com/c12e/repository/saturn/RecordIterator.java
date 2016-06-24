package com.c12e.repository.saturn;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.Iterator;

/**
 *
 * @author dschueller at cognitivescale.com
 * @since 1/14/16
 */
public class RecordIterator implements Iterator<RecordIterator.RecordData> {

    private ResultSet results;
    private Iterator<Row> iter;

    public static class RecordData {
        public GenericRecord data;
        public String repositoryId;
        public String name;
        public String schemaId;
        public String keyid;
        public String timestamp;
    }

    public RecordIterator(ResultSet data ) {
        this.results = data;
        this.iter = data.iterator();
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public RecordData next() {
        if (!hasNext()) throw new IllegalStateException("No next record");

        try {
            Row r = iter.next();

            SeekableByteArrayInput bais = new SeekableByteArrayInput(r.getBytes(RepositoryConnection.COL_DATA()).array());
            GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(bais, datumReader);

            if (dataFileReader.hasNext()) {
                RecordData rd = new RecordData();
                rd.data = dataFileReader.next();
                rd.repositoryId = r.getString(RepositoryConnection.COL_REPOSITORY_ID());
                rd.name = r.getString(RepositoryConnection.COL_NAME());
                rd.schemaId = r.getString(RepositoryConnection.COL_SCHEMA_ID());
                rd.keyid = r.getString(RepositoryConnection.COL_KEY_ID());
                rd.timestamp = r.getUUID(RepositoryConnection.COL_TIMESTAMP()).toString();

                return rd;

            } else {
                throw new IOException("No AVRO Record Found");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public int getFetchCount() {
        return results.getAvailableWithoutFetching();
    }
}
