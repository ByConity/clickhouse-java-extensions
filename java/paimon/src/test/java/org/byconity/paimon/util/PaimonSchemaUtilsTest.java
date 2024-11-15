package org.byconity.paimon.util;

import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.*;
import org.byconity.proto.PaimonMeta;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class PaimonSchemaUtilsTest {

    private void checkOnlyOneTypeSet(PaimonMeta.Type type) {
        int cnt = 0;
        if (type.hasArrayType()) {
            cnt++;
        }
        if (type.hasBigIntType()) {
            cnt++;
        }
        if (type.hasBinaryType()) {
            cnt++;
        }
        if (type.hasBooleanType()) {
            cnt++;
        }
        if (type.hasCharType()) {
            cnt++;
        }
        if (type.hasDateType()) {
            cnt++;
        }
        if (type.hasDecimalType()) {
            cnt++;
        }
        if (type.hasDoubleType()) {
            cnt++;
        }
        if (type.hasFloatType()) {
            cnt++;
        }
        if (type.hasIntType()) {
            cnt++;
        }
        if (type.hasLocalZonedTimestampType()) {
            cnt++;
        }
        if (type.hasMapType()) {
            cnt++;
        }
        if (type.hasRowType()) {
            cnt++;
        }
        if (type.hasSmallIntType()) {
            cnt++;
        }
        if (type.hasTimestampType()) {
            cnt++;
        }
        if (type.hasTinyIntType()) {
            cnt++;
        }
        if (type.hasVarBinaryType()) {
            cnt++;
        }
        if (type.hasVarCharType()) {
            cnt++;
        }
        Assert.assertEquals(1, cnt);
    }

    @Test
    public void testNullableScalarType() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("col_bigint", DataTypes.BIGINT());
        schemaBuilder.column("col_binary", DataTypes.BINARY(15));
        schemaBuilder.column("col_boolean", DataTypes.BOOLEAN());
        schemaBuilder.column("col_char", DataTypes.CHAR(32));
        schemaBuilder.column("col_date", DataTypes.DATE());
        schemaBuilder.column("col_decimal", DataTypes.DECIMAL(12, 5));
        schemaBuilder.column("col_double", DataTypes.DOUBLE());
        schemaBuilder.column("col_float", DataTypes.FLOAT());
        schemaBuilder.column("col_int", DataTypes.INT());
        schemaBuilder.column("col_local_zoned_timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
        schemaBuilder.column("col_smallint", DataTypes.SMALLINT());
        schemaBuilder.column("col_timestamp", DataTypes.TIMESTAMP());
        schemaBuilder.column("col_tinyint", DataTypes.TINYINT());
        schemaBuilder.column("col_varbinary", DataTypes.VARBINARY(128));
        schemaBuilder.column("col_varchar", DataTypes.VARCHAR(128));
        schemaBuilder.primaryKey("col_int", "col_varchar");
        schemaBuilder.partitionKeys("col_int");
        Schema schema = schemaBuilder.build();

        PaimonMeta.Schema proto = PaimonSchemaUtils.toProto(schema);
        Assert.assertEquals(2, proto.getPrimaryKeysCount());
        Assert.assertEquals("col_int", proto.getPrimaryKeys(0));
        Assert.assertEquals("col_varchar", proto.getPrimaryKeys(1));
        Assert.assertEquals(1, proto.getPartitionKeysCount());
        Assert.assertEquals("col_int", proto.getPartitionKeys(0));
        {
            PaimonMeta.Field fieldBigInt = proto.getFields(0);
            Assert.assertEquals("col_bigint", fieldBigInt.getName());
            PaimonMeta.Type type = fieldBigInt.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasBigIntType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldBinary = proto.getFields(1);
            Assert.assertEquals("col_binary", fieldBinary.getName());
            PaimonMeta.Type type = fieldBinary.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasBinaryType());
            checkOnlyOneTypeSet(type);
            Assert.assertEquals(15, type.getBinaryType().getLength());
        }
        {
            PaimonMeta.Field fieldBoolean = proto.getFields(2);
            Assert.assertEquals("col_boolean", fieldBoolean.getName());
            PaimonMeta.Type type = fieldBoolean.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasBooleanType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldChar = proto.getFields(3);
            Assert.assertEquals("col_char", fieldChar.getName());
            PaimonMeta.Type type = fieldChar.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasCharType());
            checkOnlyOneTypeSet(type);
            Assert.assertEquals(32, type.getCharType().getLength());
        }
        {
            PaimonMeta.Field fieldDate = proto.getFields(4);
            Assert.assertEquals("col_date", fieldDate.getName());
            PaimonMeta.Type type = fieldDate.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasDateType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldDecimal = proto.getFields(5);
            Assert.assertEquals("col_decimal", fieldDecimal.getName());
            PaimonMeta.Type type = fieldDecimal.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasDecimalType());
            checkOnlyOneTypeSet(type);
            Assert.assertEquals(12, type.getDecimalType().getPrecision());
            Assert.assertEquals(5, type.getDecimalType().getScale());
        }
        {
            PaimonMeta.Field fieldDouble = proto.getFields(6);
            Assert.assertEquals("col_double", fieldDouble.getName());
            PaimonMeta.Type type = fieldDouble.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasDoubleType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldFloat = proto.getFields(7);
            Assert.assertEquals("col_float", fieldFloat.getName());
            PaimonMeta.Type type = fieldFloat.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasFloatType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldInt = proto.getFields(8);
            Assert.assertEquals("col_int", fieldInt.getName());
            PaimonMeta.Type type = fieldInt.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasIntType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldLocalZonedTimestamp = proto.getFields(9);
            Assert.assertEquals("col_local_zoned_timestamp", fieldLocalZonedTimestamp.getName());
            PaimonMeta.Type type = fieldLocalZonedTimestamp.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasLocalZonedTimestampType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldSmallInt = proto.getFields(10);
            Assert.assertEquals("col_smallint", fieldSmallInt.getName());
            PaimonMeta.Type type = fieldSmallInt.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasSmallIntType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldTimestamp = proto.getFields(11);
            Assert.assertEquals("col_timestamp", fieldTimestamp.getName());
            PaimonMeta.Type type = fieldTimestamp.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasTimestampType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldTinyInt = proto.getFields(12);
            Assert.assertEquals("col_tinyint", fieldTinyInt.getName());
            PaimonMeta.Type type = fieldTinyInt.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasTinyIntType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldVarBinary = proto.getFields(13);
            Assert.assertEquals("col_varbinary", fieldVarBinary.getName());
            PaimonMeta.Type type = fieldVarBinary.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasVarBinaryType());
            checkOnlyOneTypeSet(type);
            Assert.assertEquals(128, type.getVarBinaryType().getLength());
        }
        {
            PaimonMeta.Field fieldVarChar = proto.getFields(14);
            Assert.assertEquals("col_varchar", fieldVarChar.getName());
            PaimonMeta.Type type = fieldVarChar.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasVarCharType());
            checkOnlyOneTypeSet(type);
            Assert.assertEquals(128, type.getVarCharType().getLength());
        }
    }

    @Test
    public void testNonNullableScalarType() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("col_bigint", new BigIntType(false));
        schemaBuilder.column("col_binary", new BinaryType(false, 4096));
        schemaBuilder.column("col_boolean", new BooleanType(false));
        schemaBuilder.column("col_char", new CharType(false, 1024));
        schemaBuilder.column("col_date", new DateType(false));
        schemaBuilder.column("col_decimal", new DecimalType(false, 32, 16));
        schemaBuilder.column("col_double", new DoubleType(false));
        schemaBuilder.column("col_float", new FloatType(false));
        schemaBuilder.column("col_int", new IntType(false));
        schemaBuilder.column("col_local_zoned_timestamp", new LocalZonedTimestampType(false, 5));
        schemaBuilder.column("col_smallint", new SmallIntType(false));
        schemaBuilder.column("col_timestamp", new TimestampType(false, 3));
        schemaBuilder.column("col_tinyint", new TinyIntType(false));
        schemaBuilder.column("col_varbinary", new VarBinaryType(false, 8192));
        schemaBuilder.column("col_varchar", new VarCharType(false, 512));
        schemaBuilder.primaryKey("col_int", "col_varchar");
        schemaBuilder.partitionKeys("col_int");
        Schema schema = schemaBuilder.build();

        PaimonMeta.Schema proto = PaimonSchemaUtils.toProto(schema);
        Assert.assertEquals(2, proto.getPrimaryKeysCount());
        Assert.assertEquals("col_int", proto.getPrimaryKeys(0));
        Assert.assertEquals("col_varchar", proto.getPrimaryKeys(1));
        Assert.assertEquals(1, proto.getPartitionKeysCount());
        Assert.assertEquals("col_int", proto.getPartitionKeys(0));
        {
            PaimonMeta.Field fieldBigInt = proto.getFields(0);
            Assert.assertEquals("col_bigint", fieldBigInt.getName());
            PaimonMeta.Type type = fieldBigInt.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasBigIntType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldBinary = proto.getFields(1);
            Assert.assertEquals("col_binary", fieldBinary.getName());
            PaimonMeta.Type type = fieldBinary.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasBinaryType());
            checkOnlyOneTypeSet(type);
            Assert.assertEquals(4096, type.getBinaryType().getLength());
        }
        {
            PaimonMeta.Field fieldBoolean = proto.getFields(2);
            Assert.assertEquals("col_boolean", fieldBoolean.getName());
            PaimonMeta.Type type = fieldBoolean.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasBooleanType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldChar = proto.getFields(3);
            Assert.assertEquals("col_char", fieldChar.getName());
            PaimonMeta.Type type = fieldChar.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasCharType());
            checkOnlyOneTypeSet(type);
            Assert.assertEquals(1024, type.getCharType().getLength());
        }
        {
            PaimonMeta.Field fieldDate = proto.getFields(4);
            Assert.assertEquals("col_date", fieldDate.getName());
            PaimonMeta.Type type = fieldDate.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasDateType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldDecimal = proto.getFields(5);
            Assert.assertEquals("col_decimal", fieldDecimal.getName());
            PaimonMeta.Type type = fieldDecimal.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasDecimalType());
            checkOnlyOneTypeSet(type);
            Assert.assertEquals(32, type.getDecimalType().getPrecision());
            Assert.assertEquals(16, type.getDecimalType().getScale());
        }
        {
            PaimonMeta.Field fieldDouble = proto.getFields(6);
            Assert.assertEquals("col_double", fieldDouble.getName());
            PaimonMeta.Type type = fieldDouble.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasDoubleType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldFloat = proto.getFields(7);
            Assert.assertEquals("col_float", fieldFloat.getName());
            PaimonMeta.Type type = fieldFloat.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasFloatType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldInt = proto.getFields(8);
            Assert.assertEquals("col_int", fieldInt.getName());
            PaimonMeta.Type type = fieldInt.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasIntType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldLocalZonedTimestamp = proto.getFields(9);
            Assert.assertEquals("col_local_zoned_timestamp", fieldLocalZonedTimestamp.getName());
            PaimonMeta.Type type = fieldLocalZonedTimestamp.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasLocalZonedTimestampType());
            checkOnlyOneTypeSet(type);
            Assert.assertEquals(5, type.getLocalZonedTimestampType().getPrecision());
        }
        {
            PaimonMeta.Field fieldSmallInt = proto.getFields(10);
            Assert.assertEquals("col_smallint", fieldSmallInt.getName());
            PaimonMeta.Type type = fieldSmallInt.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasSmallIntType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldTimestamp = proto.getFields(11);
            Assert.assertEquals("col_timestamp", fieldTimestamp.getName());
            PaimonMeta.Type type = fieldTimestamp.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasTimestampType());
            checkOnlyOneTypeSet(type);
            Assert.assertEquals(3, type.getTimestampType().getPrecision());
        }
        {
            PaimonMeta.Field fieldTinyInt = proto.getFields(12);
            Assert.assertEquals("col_tinyint", fieldTinyInt.getName());
            PaimonMeta.Type type = fieldTinyInt.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasTinyIntType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldVarBinary = proto.getFields(13);
            Assert.assertEquals("col_varbinary", fieldVarBinary.getName());
            PaimonMeta.Type type = fieldVarBinary.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasVarBinaryType());
            checkOnlyOneTypeSet(type);
            Assert.assertEquals(8192, type.getVarBinaryType().getLength());
        }
        {
            PaimonMeta.Field fieldVarChar = proto.getFields(14);
            Assert.assertEquals("col_varchar", fieldVarChar.getName());
            PaimonMeta.Type type = fieldVarChar.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasVarCharType());
            checkOnlyOneTypeSet(type);
            Assert.assertEquals(512, type.getVarCharType().getLength());
        }
    }

    @Test
    public void testComplexType() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("col_int", DataTypes.INT());
        schemaBuilder.column("col_array_not_null", new ArrayType(false, DataTypes.VARCHAR(16)));
        schemaBuilder.column("col_array_null", DataTypes.ARRAY(DataTypes.VARCHAR(16)));
        schemaBuilder.column("col_map_not_null",
                new MapType(false, DataTypes.TINYINT(), DataTypes.VARCHAR(16)));
        schemaBuilder.column("col_map_null",
                DataTypes.MAP(DataTypes.VARCHAR(16), DataTypes.DATE()));
        schemaBuilder.column("col_nested_1", new ArrayType(true,
                new MapType(false, DataTypes.INT(), new ArrayType(false, DataTypes.FLOAT()))));
        schemaBuilder.column("col_nested_2",
                new MapType(false, new TimestampType(false, 3),
                        new MapType(true, new BooleanType(true),
                                new ArrayType(false, new DecimalType(false, 9, 3)))));
        schemaBuilder.column("col_nested_3",
                new RowType(true, Arrays.asList(
                        new DataField(0, "field_array", new ArrayType(false, new RowType(false,
                                Arrays.asList(new DataField(0, "nested_field_int", DataTypes.INT()),
                                        new DataField(1, "nested_field_double",
                                                DataTypes.DOUBLE()))))),
                        new DataField(1, "field_map",
                                new MapType(true, DataTypes.SMALLINT(), DataTypes.CHAR(50))))));

        schemaBuilder.primaryKey("col_int");
        Schema schema = schemaBuilder.build();
        PaimonMeta.Schema proto = PaimonSchemaUtils.toProto(schema);
        Assert.assertEquals(1, proto.getPrimaryKeysCount());
        Assert.assertEquals("col_int", proto.getPrimaryKeys(0));
        Assert.assertEquals(0, proto.getPartitionKeysCount());
        {
            PaimonMeta.Field fieldInt = proto.getFields(0);
            Assert.assertEquals("col_int", fieldInt.getName());
            PaimonMeta.Type type = fieldInt.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasIntType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldArrayNotNull = proto.getFields(1);
            Assert.assertEquals("col_array_not_null", fieldArrayNotNull.getName());
            PaimonMeta.Type type = fieldArrayNotNull.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasArrayType());
            checkOnlyOneTypeSet(type);

            PaimonMeta.Type elementType = type.getArrayType().getElementType();
            Assert.assertTrue(elementType.getIsNullable());
            Assert.assertTrue(elementType.hasVarCharType());
            checkOnlyOneTypeSet(type);
        }
        {
            PaimonMeta.Field fieldArrayNull = proto.getFields(2);
            Assert.assertEquals("col_array_null", fieldArrayNull.getName());
            PaimonMeta.Type type = fieldArrayNull.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasArrayType());
            checkOnlyOneTypeSet(type);

            PaimonMeta.Type elementType = type.getArrayType().getElementType();
            Assert.assertTrue(elementType.getIsNullable());
            Assert.assertTrue(elementType.hasVarCharType());
            checkOnlyOneTypeSet(type);
            Assert.assertEquals(16, elementType.getVarCharType().getLength());
        }
        {
            PaimonMeta.Field fieldMapNotNull = proto.getFields(3);
            Assert.assertEquals("col_map_not_null", fieldMapNotNull.getName());
            PaimonMeta.Type type = fieldMapNotNull.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasMapType());
            checkOnlyOneTypeSet(type);

            PaimonMeta.Type keyType = type.getMapType().getKeyType();
            Assert.assertTrue(keyType.getIsNullable());
            Assert.assertTrue(keyType.hasTinyIntType());
            checkOnlyOneTypeSet(keyType);

            PaimonMeta.Type valueType = type.getMapType().getValueType();
            Assert.assertTrue(valueType.getIsNullable());
            Assert.assertTrue(valueType.hasVarCharType());
            checkOnlyOneTypeSet(valueType);
            Assert.assertEquals(16, valueType.getVarCharType().getLength());
        }
        {
            PaimonMeta.Field fieldMapNull = proto.getFields(4);
            Assert.assertEquals("col_map_null", fieldMapNull.getName());
            PaimonMeta.Type type = fieldMapNull.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasMapType());
            checkOnlyOneTypeSet(type);

            PaimonMeta.Type keyType = type.getMapType().getKeyType();
            Assert.assertTrue(keyType.getIsNullable());
            Assert.assertTrue(keyType.hasVarCharType());
            checkOnlyOneTypeSet(keyType);
            Assert.assertEquals(16, keyType.getVarCharType().getLength());

            PaimonMeta.Type valueType = type.getMapType().getValueType();
            Assert.assertTrue(valueType.getIsNullable());
            Assert.assertTrue(valueType.hasDateType());
            checkOnlyOneTypeSet(valueType);
        }
        {
            PaimonMeta.Field fieldNested1 = proto.getFields(5);
            Assert.assertEquals("col_nested_1", fieldNested1.getName());
            PaimonMeta.Type type = fieldNested1.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasArrayType());
            checkOnlyOneTypeSet(type);

            PaimonMeta.Type elementType = type.getArrayType().getElementType();
            Assert.assertFalse(elementType.getIsNullable());
            Assert.assertTrue(elementType.hasMapType());
            checkOnlyOneTypeSet(elementType);

            PaimonMeta.Type nestedKeyType = elementType.getMapType().getKeyType();
            Assert.assertTrue(nestedKeyType.getIsNullable());
            Assert.assertTrue(nestedKeyType.hasIntType());
            checkOnlyOneTypeSet(nestedKeyType);

            PaimonMeta.Type nestedValueType = elementType.getMapType().getValueType();
            Assert.assertFalse(nestedValueType.getIsNullable());
            Assert.assertTrue(nestedValueType.hasArrayType());
            checkOnlyOneTypeSet(nestedValueType);

            PaimonMeta.Type nestedNestedElementType =
                    nestedValueType.getArrayType().getElementType();
            Assert.assertTrue(nestedNestedElementType.getIsNullable());
            Assert.assertTrue(nestedNestedElementType.hasFloatType());
            checkOnlyOneTypeSet(nestedNestedElementType);
        }
        {
            PaimonMeta.Field fieldNested2 = proto.getFields(6);
            Assert.assertEquals("col_nested_2", fieldNested2.getName());
            PaimonMeta.Type type = fieldNested2.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasMapType());
            checkOnlyOneTypeSet(type);

            PaimonMeta.Type keyType = type.getMapType().getKeyType();
            Assert.assertFalse(keyType.getIsNullable());
            Assert.assertTrue(keyType.hasTimestampType());
            checkOnlyOneTypeSet(keyType);
            Assert.assertEquals(3, keyType.getTimestampType().getPrecision());

            PaimonMeta.Type valueType = type.getMapType().getValueType();
            Assert.assertTrue(valueType.getIsNullable());
            Assert.assertTrue(valueType.hasMapType());
            checkOnlyOneTypeSet(valueType);

            PaimonMeta.Type nestedKeyType = valueType.getMapType().getKeyType();
            Assert.assertTrue(nestedKeyType.getIsNullable());
            Assert.assertTrue(nestedKeyType.hasBooleanType());
            checkOnlyOneTypeSet(nestedKeyType);

            PaimonMeta.Type nestedValueType = valueType.getMapType().getValueType();
            Assert.assertFalse(nestedValueType.getIsNullable());
            Assert.assertTrue(nestedValueType.hasArrayType());
            checkOnlyOneTypeSet(nestedValueType);

            PaimonMeta.Type nestedNestedElementType =
                    nestedValueType.getArrayType().getElementType();
            Assert.assertFalse(nestedNestedElementType.getIsNullable());
            Assert.assertTrue(nestedNestedElementType.hasDecimalType());
            checkOnlyOneTypeSet(nestedNestedElementType);
        }
        {
            PaimonMeta.Field fieldNested3 = proto.getFields(7);
            Assert.assertEquals("col_nested_3", fieldNested3.getName());
            PaimonMeta.Type type = fieldNested3.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasRowType());
            checkOnlyOneTypeSet(type);

            PaimonMeta.Type fieldType1 = type.getRowType().getFields(0).getType();
            Assert.assertFalse(fieldType1.getIsNullable());
            Assert.assertTrue(fieldType1.hasArrayType());
            checkOnlyOneTypeSet(fieldType1);

            PaimonMeta.Type nestedRowType = fieldType1.getArrayType().getElementType();
            Assert.assertFalse(nestedRowType.getIsNullable());
            Assert.assertTrue(nestedRowType.hasRowType());
            checkOnlyOneTypeSet(nestedRowType);

            PaimonMeta.Type nestedFieldType1 = nestedRowType.getRowType().getFields(0).getType();
            Assert.assertTrue(nestedFieldType1.getIsNullable());
            Assert.assertTrue(nestedFieldType1.hasIntType());
            checkOnlyOneTypeSet(nestedFieldType1);

            PaimonMeta.Type nestedFieldType2 = nestedRowType.getRowType().getFields(1).getType();
            Assert.assertTrue(nestedFieldType2.getIsNullable());
            Assert.assertTrue(nestedFieldType2.hasDoubleType());
            checkOnlyOneTypeSet(nestedFieldType2);

            PaimonMeta.Type fieldType2 = type.getRowType().getFields(1).getType();
            Assert.assertTrue(fieldType2.getIsNullable());
            Assert.assertTrue(fieldType2.hasMapType());
            checkOnlyOneTypeSet(fieldType2);

            PaimonMeta.Type nestedKeyType = fieldType2.getMapType().getKeyType();
            Assert.assertTrue(nestedKeyType.getIsNullable());
            Assert.assertTrue(nestedKeyType.hasSmallIntType());
            checkOnlyOneTypeSet(nestedKeyType);

            PaimonMeta.Type nestedValueType = fieldType2.getMapType().getValueType();
            Assert.assertTrue(nestedValueType.getIsNullable());
            Assert.assertTrue(nestedValueType.hasCharType());
            checkOnlyOneTypeSet(nestedValueType);
        }
    }
}
