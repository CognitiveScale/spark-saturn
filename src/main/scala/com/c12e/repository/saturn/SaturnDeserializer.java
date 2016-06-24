package com.c12e.repository.saturn;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.UUID;
import java.util.logging.Logger;

/**
 *
 * @author dschueller at cognitivescale.com
 * @since 1/12/16
 */
public class SaturnDeserializer extends RepositoryConnection {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = Logger.getLogger(SaturnDeserializer.class.getName());

    private Select query;
    private ResultSet results;
    private RepositoryConf conf;

    public SaturnDeserializer(RepositoryConf conf) throws SchemaIncompatibleException {
        super(conf);
        this.conf = conf;

        Select.Where where = createBaseQuery();
        this.query = withVersionOrdering(where);

        if (conf.version().isDefined()) {
            where.and(QueryBuilder.eq(RepositoryConnection.COL_TIMESTAMP(), UUID.fromString(conf.getVersion())));
        }
        else {
            String version = queryLatestVersion();
            if (version == null) {
                // This only happens if there is no data...log it and move on, the query will return an empty result set
                LOG.warning("Attempt to read the latest version of a non-existent DataFrame");
            }
            else {
                where.and(QueryBuilder.eq(RepositoryConnection.COL_TIMESTAMP(), UUID.fromString(version)));
            }
        }

//        this.query = withVersionOrdering(where);

//        if (conf.getSchemaId() != null)
//            this.query = this.query.and(QueryBuilder.eq(RepositoryConnection.COL_SCHEMA_ID(), conf.getSchemaId()));

        int fetchSize = conf.getFetchSize();
        if (fetchSize > 0)
            this.query.setFetchSize(fetchSize);

        Schema readSchema = (conf.schema().isDefined()) ? new Schema.Parser().parse(conf.getSchema()) : null;

        if (readSchema != null) {
            Schema dataSchema = queryDataSchema();
            SchemaCompatibility.SchemaPairCompatibility schemaCompatibility = SchemaCompatibility.checkReaderWriterCompatibility(dataSchema, readSchema);

            if (schemaCompatibility.getType() != SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE) {
                close();
                String s = SchemaConverter.analyzeSchemaDifferences(new Schema.Field("root", readSchema, "", null), readSchema, new Schema.Field("root", dataSchema, "", null), dataSchema);
                throw new SchemaIncompatibleException(String.format("\n*** Schema Compatibility Failure - %s, %s ***\n%s", conf.getName(), conf.getSchemaId(), s));
            }
        }

        System.out.println(query.toString());
        this.results = session().execute(query);
    }

    private Select withVersionOrdering(Select.Where where) {
        return where.orderBy(
            QueryBuilder.desc(RepositoryConnection.COL_TIMESTAMP()),
            QueryBuilder.desc(RepositoryConnection.COL_KEY_ID()));
    }

    Schema queryDataSchema() {
        ResultSet onerec = session().execute(query.limit(1));
        ByteBuffer data = onerec.one().getBytes(RepositoryConnection.COL_DATA());

        try {
            GenericRecord record = readAvroRecord(data.array());
            return record.getSchema();
        }
        catch (IOException e) {
            return Schema.create(Schema.Type.NULL);
        }

    }

    private String queryLatestVersion() {
        Select query = withVersionOrdering(createBaseQuery());
        ResultSet one = session().execute(query.limit(1));
        Row r = one.one();
        if (r == null) {
            return null;
        }

        return r.getUUID(RepositoryConnection.COL_TIMESTAMP()).toString();
    }

    private Select.Where createBaseQuery() {
        return QueryBuilder.select()
            .all()
            .from(keyspace(), dataTableName())
            .where(QueryBuilder.eq(RepositoryConnection.COL_REPOSITORY_ID(), conf.getRepositoryId()))
            .and(QueryBuilder.eq(RepositoryConnection.COL_NAME(), conf.getName()));
    }

    public RecordIterator iterator() {
        return new RecordIterator(results);
    }

    private GenericRecord readAvroRecord(byte[] data) throws IOException {
        SeekableByteArrayInput bais = new SeekableByteArrayInput(data);
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(bais, datumReader);

        if (dataFileReader.hasNext())
            return dataFileReader.next();
        else {
            throw new IOException("No AVRO Record Found");
        }
    }
}
