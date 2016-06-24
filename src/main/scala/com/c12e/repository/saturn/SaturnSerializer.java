package com.c12e.repository.saturn;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/**
 *
 * @author dschueller at cognitivescale.com
 * @since 1/12/16
 */
public class SaturnSerializer extends RepositoryConnection {

    private static final long serialVersionUID = 1L;

    private BoundStatement insertStmt;

    private ByteArrayOutputStream baos;
    private DataFileWriter<GenericRecord> dataFileWriter;

    private String repositoryId;
    private String name;
    private String schemaId;

    private UUID timestamp;

    private KeyHashGenerator kg;

    private Schema writeschema;

    public SaturnSerializer(RepositoryConf conf, UUID timestamp) {
        super(conf);

        this.repositoryId = conf.getRepositoryId();
        this.name = conf.getName();
        this.schemaId = conf.getSchemaId();
        this.timestamp = timestamp;

        this.writeschema = (conf.schema().isDefined()) ? new Schema.Parser().parse(conf.getSchema()) : null;

        baos = new ByteArrayOutputStream();
        dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(writeschema));

        PreparedStatement insertStmt = session().prepare("INSERT INTO " + keyspace() + "." + dataTableName() + " (repositoryid, name, timestamp, keyid, schemaid, data) VALUES (?,?,?,?,?,?);");
        this.insertStmt = new BoundStatement(insertStmt);

        try {
            kg = new KeyHashGenerator();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    private void updateKeyHash( GenericRecord r ) {

        Schema s = r.getSchema();

        for(Schema.Field f : s.getFields()) {

            Object val = r.get(f.name());

            if(f.schema().getType() == Schema.Type.RECORD)
                updateKeyHash((GenericRecord)val);
            else
                kg.updateHash(f,val);
        }

    }

    public void serializeRecord( GenericRecord r ) throws SchemaIncompatibleException {

        try {

            if(writeschema != null) {
                Schema dataSchema = r.getSchema();
                SchemaCompatibility.SchemaPairCompatibility schemaCompatibility = SchemaCompatibility.checkReaderWriterCompatibility(dataSchema, writeschema);

                if(schemaCompatibility.getType() != SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE) {
                    close();
                    String s = SchemaConverter.analyzeSchemaDifferences(new Schema.Field("root", writeschema, "", null), writeschema, new Schema.Field("root", dataSchema, "", null), dataSchema);
                    throw new SchemaIncompatibleException(String.format("\n*** Schema Compatibility Failure - %s, %s, %s ***\n%s", name, schemaId, timestamp.toString(), s));
                }
            }

            kg.reset();
            baos.reset();

            updateKeyHash(r);

            dataFileWriter.create(r.getSchema(), baos);
            dataFileWriter.append(r);
            dataFileWriter.close();

            session().execute(insertStmt.bind(repositoryId, name, timestamp, kg.getHash(), schemaId, ByteBuffer.wrap(baos.toByteArray())));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
