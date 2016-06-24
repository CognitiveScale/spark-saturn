package com.c12e.repository.saturn;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaCompatibility;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dschueller on 11/20/15.
 */
public class SchemaConverter {

    private static Schema getFieldSchema(String name, StructField fld) throws UnsupportedTypeException {

        if (fld.dataType() instanceof StructType) {

            SchemaBuilder.FieldAssembler<Schema> objFlds = (fld.nullable()) ? SchemaBuilder.nullable().record(sanitizeFieldname(name)).fields() : SchemaBuilder.record(sanitizeFieldname(name)).fields();

            StructType dataType = (StructType) fld.dataType();

            for (StructField f : dataType.fields()) {
                String fldName = f.name();
                Schema fs = getFieldSchema(fldName, f);
                objFlds.name(sanitizeFieldname(fldName)).type(fs).noDefault();
            }

            return objFlds.endRecord();
        } else if (fld.dataType() instanceof ArrayType) {
            ArrayType dataType = (ArrayType) fld.dataType();
            StructField elementField = new StructField(fld.name(), dataType.elementType(), fld.nullable(), Metadata.empty());

            if (fld.nullable()) {
                return SchemaBuilder.nullable().array().items(getFieldSchema(name, elementField));
            } else {
                return SchemaBuilder.array().items(getFieldSchema(name, elementField));
            }
        } else if (fld.dataType() instanceof BooleanType)
            return SchemaBuilder.nullable().booleanType();
        else if (fld.dataType() instanceof DoubleType)
            return SchemaBuilder.nullable().doubleType();
        else if (fld.dataType() instanceof FloatType)
            return SchemaBuilder.nullable().floatType();
        else if (fld.dataType() instanceof IntegerType)
            return SchemaBuilder.nullable().intType();
        else if (fld.dataType() instanceof LongType)
            return SchemaBuilder.nullable().longType();
        else if (fld.dataType() instanceof StringType)
            return SchemaBuilder.nullable().stringType();
        else if (fld.dataType() instanceof BinaryType)
            return SchemaBuilder.nullable().bytesType();
        else if (fld.dataType() instanceof DateType)
            return SchemaBuilder.nullable().longType();
        else {
            /// TODO: Create an exception type for this
            throw new UnsupportedTypeException("Unsupported type " + fld.toString());
        }
    }

    private static String sanitizeFieldname(String fieldName) {

        String fName = fieldName.replaceAll(" ", "_");
        fName = fName.replaceAll("[^_a-zA-Z0-9\\s]", "");

        if (Character.isDigit(fName.charAt(0)))
            fName = "_" + fName;

        return fName;
    }

    private static class SparkSchemaType {

        public SparkSchemaType(DataType type, boolean nullable ) {
            this.nullable = nullable;
            this.type = type;
        }

        public boolean  nullable;
        public DataType type;
    }

    private static SparkSchemaType AvroToSparkType( Schema avroSchema ) throws UnsupportedTypeException {

        switch(avroSchema.getType()) {

            case INT:
                return new SparkSchemaType(DataTypes.IntegerType, false);
            case STRING:
                return new SparkSchemaType(DataTypes.StringType, false);
            case BOOLEAN:
                return new SparkSchemaType(DataTypes.BooleanType, false);
            case BYTES:
                return new SparkSchemaType(DataTypes.BinaryType, false);
            case DOUBLE:
                return new SparkSchemaType(DataTypes.DoubleType, false);
            case FLOAT:
                return new SparkSchemaType(DataTypes.FloatType, false);
            case LONG:
                return new SparkSchemaType(DataTypes.LongType, false);
            case FIXED:
                return new SparkSchemaType(DataTypes.BinaryType, false);
            case ENUM:
                return new SparkSchemaType(DataTypes.StringType, false);

            case RECORD:
                ArrayList<StructField> recFields = new ArrayList<>();

                for( Schema.Field f : avroSchema.getFields() ) {
                    SparkSchemaType sqlType = AvroToSparkType( f.schema() );
                    StructField sf = new StructField(f.name(), sqlType.type, sqlType.nullable, Metadata.empty());
                    recFields.add( sf );
                }

                return new SparkSchemaType(DataTypes.createStructType(recFields), false);

            case ARRAY:
                SparkSchemaType schemaType = AvroToSparkType(avroSchema.getElementType());
                return new SparkSchemaType( DataTypes.createArrayType( schemaType.type, schemaType.nullable ), false);

            case UNION:
                return new SparkSchemaType( AvroToSparkType(avroSchema.getTypes().get(0)).type, true );
        }

        throw new UnsupportedTypeException("Unsupported type " + avroSchema.getType().getName());
    }

    public static String analyzeSchemaDifferences(Schema.Field readerField, Schema reader, Schema.Field writerField, Schema writer) {

        String result = "";

        int readerFields = 0;
        int writerFields = 0;

        String readerNamespace = "";
        String writerNamespace = "";

        if (reader.getType() == Schema.Type.RECORD) {
            readerNamespace = reader.getNamespace();
            readerFields = reader.getFields().size();
        }

        if (writer.getType() == Schema.Type.RECORD) {
            writerNamespace = writer.getNamespace();
            writerFields = writer.getFields().size();
        }

        if (writerNamespace != readerNamespace)
            return String.format("Field name mismatch - reader field: %s  reader name: %s  writer field: %s  writer name: %s", readerField.name(), readerNamespace, writerField.name(), writerNamespace);
        else if (writerFields != readerFields)
            return String.format("Field number mismatch - reader: (%d)  writer: (%d)", readerFields, writerFields);
        else if (readerField == null)
            return "Reader field not found for writer field " + writerField.name() + "\n";
        else if (writerField == null)
            return "Writer field not found for reader field " + readerField.name() + "\n";
        else if (reader.getType() == Schema.Type.UNION)
            return analyzeSchemaDifferences(readerField, reader.getTypes().get(0), writerField, writer);
        else if (writer.getType() == Schema.Type.UNION)
            return analyzeSchemaDifferences(readerField, reader, writerField, writer.getTypes().get(0));
        if (reader.getType() == Schema.Type.ARRAY)
            return analyzeSchemaDifferences(readerField, reader.getElementType(), writerField, writer);
        else if (writer.getType() == Schema.Type.ARRAY)
            return analyzeSchemaDifferences(readerField, reader, writerField, writer.getElementType());
        else if (reader.getType() == Schema.Type.RECORD) {
            List<Schema.Field> fields = writer.getFields();

            for (Schema.Field f : fields) {

                Schema fs = f.schema();

                Schema.Field wf = SchemaCompatibility.lookupWriterField(reader, f);

                if (wf != null)
                    result += analyzeSchemaDifferences(f, fs, wf, wf.schema());
                else
                    result += analyzeSchemaDifferences(f, fs, null, null);
            }
        }

        return result;
    }

    public static StructType toSparkType( Schema avroSchema ) throws UnsupportedTypeException {
        return (StructType) AvroToSparkType(avroSchema).type;
    }

    public static Schema generateAvroSchema(String type, StructType st) throws UnsupportedTypeException {

        SchemaBuilder.RecordBuilder<Schema> foobar = SchemaBuilder.record(type);
        SchemaBuilder.FieldAssembler<Schema> objFlds = foobar.fields();

        for (StructField f : st.fields()) {

            String fldName = f.name();
            Schema fs = getFieldSchema(fldName, f);
            objFlds = objFlds.name(fldName).type(fs).noDefault();

        }

        return objFlds.endRecord();
    }

}
