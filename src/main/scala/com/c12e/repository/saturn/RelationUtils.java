package com.c12e.repository.saturn;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;

import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;


/**
 * @author msanchez at cognitivescale.com
 * @since 6/20/16
 */
public final class RelationUtils {

    public static StructType generateDataSchema(Map<String, String> parameters) throws SchemaIncompatibleException, UnsupportedTypeException {
        SaturnDeserializer sat = new SaturnDeserializer(RepositoryConf.fromParams(parameters));
        StructType dataSchema = SchemaConverter.toSparkType(sat.queryDataSchema());
        return dataSchema
            .add("saturn_repositoryid", DataTypes.StringType, false, Metadata.empty())
            .add("saturn_name", DataTypes.StringType, false, Metadata.empty())
            .add("saturn_timestamp", DataTypes.StringType, false, Metadata.empty())
            .add("saturn_keyid", DataTypes.StringType, false, Metadata.empty());

    }

    @SuppressWarnings("unchecked")
    public static GenericRecordBuilder generateAvroRecord(Row row, Schema s) throws IOException, UnsupportedTypeException {

        GenericRecordBuilder grb = new GenericRecordBuilder(s);

        for (Schema.Field fld : s.getFields()) {

            String colName = fld.name();

            Object colVal = null;

            try {
                colVal = row.getAs(colName);
            } catch (IllegalArgumentException e) {
                //Leave colVal as NULL if it doesnt exist in the row
            }

            Schema fldSchema = nonNullType(fld.schema());

            if (fldSchema.getType() == Schema.Type.RECORD) {
                if (colVal != null) {
                    GenericRecordBuilder objBuilder = generateAvroRecord((Row) colVal, fldSchema);
                    grb.set(colName, objBuilder.build());
                } else {
                    grb.set(colName, null);
                }
            } else if (fldSchema.getType() == Schema.Type.ARRAY) {

                Schema arraySchema = nonNullType(fldSchema);
                Schema elementSchema = nonNullType(arraySchema.getElementType());
                GenericData.Array<Object> avroArray = new GenericData.Array<>(0, arraySchema);

                if (colVal != null) {

                    List<Object> elements = row.getList(row.fieldIndex(colName));

                    for (Object element : elements) {
                        if (elementSchema.getType() == Schema.Type.RECORD || elementSchema.getType() == Schema.Type.ARRAY) {
                            GenericRecordBuilder objBuilder = generateAvroRecord((Row) element, elementSchema);
                            avroArray.add(objBuilder.build());
                        } else
                            avroArray.add(element);
                    }

                }

                grb.set(colName, avroArray);
            } else if ((fldSchema.getType() == Schema.Type.LONG) && (colVal instanceof java.sql.Date)) {
                long timeVal = ((Date) colVal).getTime();
                grb.set(colName, timeVal);
            } else {
                grb.set(colName, colVal);
            }
        }

        return grb;
    }

    private static Schema nonNullType(Schema s) {
        if (s.getType() == Schema.Type.UNION)
            return s.getTypes().get(0);
        else
            return s;
    }

    @SuppressWarnings("unchecked")
    public static ArrayList<Object> buildRow(GenericRecord r) throws UnsupportedTypeException {

        ArrayList<Object> columnValues = new ArrayList<>();

        StructType rowSchema = SchemaConverter.toSparkType(r.getSchema());

        for (StructField f : rowSchema.fields()) {

            String fName = f.name();

            if (f.dataType() instanceof StructType) {
                GenericRecord rVal = (GenericRecord) r.get(fName);
                if (rVal != null) {
                    columnValues.add(RowFactory.create(buildRow(rVal).toArray()));
                } else
                    columnValues.add(null);
            } else if (f.dataType() instanceof ArrayType) {

                DataType elemType = ((ArrayType) f.dataType()).elementType();
                List<Object> vals = (List<Object>) r.get(fName);
                ArrayList<Object> arrayVals = new ArrayList<>();

                if (elemType instanceof StructType) {
                    for (Object val : vals)
                        arrayVals.add(RowFactory.create(buildRow((GenericRecord) val).toArray()));
                } else {
                    for (Object val : vals) {
                        if (val instanceof Utf8)
                            arrayVals.add(val.toString());
                        else
                            arrayVals.add(val);
                    }
                }

                columnValues.add(arrayVals.toArray());

            } else if (f.dataType() instanceof StringType) {
                Object strVal = r.get(fName);

                if (strVal instanceof Utf8)
                    columnValues.add(strVal.toString());
                else
                    columnValues.add(strVal);
            } else {
                columnValues.add(r.get(fName));
            }
        }

        return columnValues;
    }

    private RelationUtils() { }
}
