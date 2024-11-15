package org.byconity.iceberg.util;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.byconity.proto.IcebergMeta;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;


public class IcebergSchemaUtilsTest {
    private void checkOnlyOneTypeSet(IcebergMeta.Type type) {
        int cnt = 0;
        if (type.hasBooleanType()) {
            cnt++;
        }
        if (type.hasIntegerType()) {
            cnt++;
        }
        if (type.hasLongType()) {
            cnt++;
        }
        if (type.hasFloatType()) {
            cnt++;
        }
        if (type.hasDoubleType()) {
            cnt++;
        }
        if (type.hasDateType()) {
            cnt++;
        }
        if (type.hasTimestampType()) {
            cnt++;
        }
        if (type.hasStringType()) {
            cnt++;
        }
        if (type.hasUUIDType()) {
            cnt++;
        }
        if (type.hasFixedType()) {
            cnt++;
        }
        if (type.hasBinaryType()) {
            cnt++;
        }
        if (type.hasDecimalType()) {
            cnt++;
        }
        if (type.hasListType()) {
            cnt++;
        }
        if (type.hasMapType()) {
            cnt++;
        }
        if (type.hasStructType()) {
            cnt++;
        }
        Assert.assertEquals(1, cnt);
    }

    @Test
    public void testNullableScalarType() {
        int id = 1;
        Schema schema =
                new Schema(Types.NestedField.optional(id++, "col_boolean", Types.BooleanType.get()),
                        Types.NestedField.optional(id++, "col_integer", Types.IntegerType.get()),
                        Types.NestedField.optional(id++, "col_long", Types.LongType.get()),
                        Types.NestedField.optional(id++, "col_float", Types.FloatType.get()),
                        Types.NestedField.optional(id++, "col_double", Types.DoubleType.get()),
                        Types.NestedField.optional(id++, "col_date", Types.DateType.get()),
                        Types.NestedField.optional(id++, "col_timestamp",
                                Types.TimestampType.withoutZone()),
                        Types.NestedField.optional(id++, "col_string", Types.StringType.get()),
                        Types.NestedField.optional(id++, "col_uuid", Types.UUIDType.get()),
                        Types.NestedField.optional(id++, "col_fixed", Types.FixedType.ofLength(15)),
                        Types.NestedField.optional(id++, "col_binary", Types.BinaryType.get()),
                        Types.NestedField.optional(id, "col_decimal", Types.DecimalType.of(12, 5)));

        PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(schema);
        for (String partitionKey : Collections.singleton("col_integer")) {
            partitionSpecBuilder.identity(partitionKey);
        }
        PartitionSpec spec = partitionSpecBuilder.build();

        IcebergMeta.Schema proto = IcebergSchemaUtils.toProto(schema, spec, null);
        Assert.assertEquals(1, proto.getPartitionKeysCount());
        Assert.assertEquals("col_integer", proto.getPartitionKeys(0));
        {
            IcebergMeta.Field fieldBoolean = proto.getFields(0);
            Assert.assertEquals("col_boolean", fieldBoolean.getName());
            IcebergMeta.Type type = fieldBoolean.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasBooleanType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldInt = proto.getFields(1);
            Assert.assertEquals("col_integer", fieldInt.getName());
            IcebergMeta.Type type = fieldInt.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasIntegerType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldBigInt = proto.getFields(2);
            Assert.assertEquals("col_long", fieldBigInt.getName());
            IcebergMeta.Type type = fieldBigInt.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasLongType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldFloat = proto.getFields(3);
            Assert.assertEquals("col_float", fieldFloat.getName());
            IcebergMeta.Type type = fieldFloat.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasFloatType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldDouble = proto.getFields(4);
            Assert.assertEquals("col_double", fieldDouble.getName());
            IcebergMeta.Type type = fieldDouble.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasDoubleType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldDate = proto.getFields(5);
            Assert.assertEquals("col_date", fieldDate.getName());
            IcebergMeta.Type type = fieldDate.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasDateType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldTimestamp = proto.getFields(6);
            Assert.assertEquals("col_timestamp", fieldTimestamp.getName());
            IcebergMeta.Type type = fieldTimestamp.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasTimestampType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldChar = proto.getFields(7);
            Assert.assertEquals("col_string", fieldChar.getName());
            IcebergMeta.Type type = fieldChar.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasStringType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldUUID = proto.getFields(8);
            Assert.assertEquals("col_uuid", fieldUUID.getName());
            IcebergMeta.Type type = fieldUUID.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasUUIDType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldFixed = proto.getFields(9);
            Assert.assertEquals("col_fixed", fieldFixed.getName());
            IcebergMeta.Type type = fieldFixed.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasFixedType());
            checkOnlyOneTypeSet(type);
            Assert.assertEquals(15, type.getFixedType().getLength());
        }
        {
            IcebergMeta.Field fieldBinary = proto.getFields(10);
            Assert.assertEquals("col_binary", fieldBinary.getName());
            IcebergMeta.Type type = fieldBinary.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasBinaryType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldDecimal = proto.getFields(11);
            Assert.assertEquals("col_decimal", fieldDecimal.getName());
            IcebergMeta.Type type = fieldDecimal.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasDecimalType());
            checkOnlyOneTypeSet(type);
            Assert.assertEquals(12, type.getDecimalType().getPrecision());
            Assert.assertEquals(5, type.getDecimalType().getScale());
        }
    }

    @Test
    public void testNonNullableScalarType() {
        int id = 1;
        Schema schema =
                new Schema(Types.NestedField.required(id++, "col_boolean", Types.BooleanType.get()),
                        Types.NestedField.required(id++, "col_integer", Types.IntegerType.get()),
                        Types.NestedField.required(id++, "col_long", Types.LongType.get()),
                        Types.NestedField.required(id++, "col_float", Types.FloatType.get()),
                        Types.NestedField.required(id++, "col_double", Types.DoubleType.get()),
                        Types.NestedField.required(id++, "col_date", Types.DateType.get()),
                        Types.NestedField.required(id++, "col_timestamp",
                                Types.TimestampType.withoutZone()),
                        Types.NestedField.required(id++, "col_string", Types.StringType.get()),
                        Types.NestedField.required(id++, "col_uuid", Types.UUIDType.get()),
                        Types.NestedField.required(id++, "col_fixed", Types.FixedType.ofLength(15)),
                        Types.NestedField.required(id++, "col_binary", Types.BinaryType.get()),
                        Types.NestedField.required(id, "col_decimal", Types.DecimalType.of(12, 5)));

        PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(schema);
        for (String partitionKey : Collections.singleton("col_integer")) {
            partitionSpecBuilder.identity(partitionKey);
        }
        PartitionSpec spec = partitionSpecBuilder.build();

        IcebergMeta.Schema proto = IcebergSchemaUtils.toProto(schema, spec, null);
        Assert.assertEquals(1, proto.getPartitionKeysCount());
        Assert.assertEquals("col_integer", proto.getPartitionKeys(0));
        {
            IcebergMeta.Field fieldBoolean = proto.getFields(0);
            Assert.assertEquals("col_boolean", fieldBoolean.getName());
            IcebergMeta.Type type = fieldBoolean.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasBooleanType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldInt = proto.getFields(1);
            Assert.assertEquals("col_integer", fieldInt.getName());
            IcebergMeta.Type type = fieldInt.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasIntegerType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldBigInt = proto.getFields(2);
            Assert.assertEquals("col_long", fieldBigInt.getName());
            IcebergMeta.Type type = fieldBigInt.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasLongType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldFloat = proto.getFields(3);
            Assert.assertEquals("col_float", fieldFloat.getName());
            IcebergMeta.Type type = fieldFloat.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasFloatType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldDouble = proto.getFields(4);
            Assert.assertEquals("col_double", fieldDouble.getName());
            IcebergMeta.Type type = fieldDouble.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasDoubleType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldDate = proto.getFields(5);
            Assert.assertEquals("col_date", fieldDate.getName());
            IcebergMeta.Type type = fieldDate.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasDateType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldTimestamp = proto.getFields(6);
            Assert.assertEquals("col_timestamp", fieldTimestamp.getName());
            IcebergMeta.Type type = fieldTimestamp.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasTimestampType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldChar = proto.getFields(7);
            Assert.assertEquals("col_string", fieldChar.getName());
            IcebergMeta.Type type = fieldChar.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasStringType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldUUID = proto.getFields(8);
            Assert.assertEquals("col_uuid", fieldUUID.getName());
            IcebergMeta.Type type = fieldUUID.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasUUIDType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldFixed = proto.getFields(9);
            Assert.assertEquals("col_fixed", fieldFixed.getName());
            IcebergMeta.Type type = fieldFixed.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasFixedType());
            checkOnlyOneTypeSet(type);
            Assert.assertEquals(15, type.getFixedType().getLength());
        }
        {
            IcebergMeta.Field fieldBinary = proto.getFields(10);
            Assert.assertEquals("col_binary", fieldBinary.getName());
            IcebergMeta.Type type = fieldBinary.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasBinaryType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldDecimal = proto.getFields(11);
            Assert.assertEquals("col_decimal", fieldDecimal.getName());
            IcebergMeta.Type type = fieldDecimal.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasDecimalType());
            checkOnlyOneTypeSet(type);
            Assert.assertEquals(12, type.getDecimalType().getPrecision());
            Assert.assertEquals(5, type.getDecimalType().getScale());
        }
    }

    @Test
    public void testComplexType() {
        int id = 1;
        Schema schema =
                new Schema(Types.NestedField.required(id++, "col_integer", Types.IntegerType.get()),
                        Types.NestedField.required(id++, "col_list_not_null",
                                Types.ListType.ofOptional(id++, Types.StringType.get())),
                        Types.NestedField.optional(id++, "col_list_null",
                                Types.ListType.ofOptional(id++, Types.StringType.get())),

                        Types.NestedField.required(id++, "col_map_not_null",
                                Types.MapType.ofOptional(id++, id++, Types.IntegerType.get(),
                                        Types.StringType.get())),
                        Types.NestedField.optional(id++, "col_map_null",
                                Types.MapType.ofOptional(id++, id++, Types.StringType.get(),
                                        Types.DateType.get())),

                        Types.NestedField.optional(id++, "col_nested_1", Types.ListType.ofRequired(
                                id++,
                                Types.MapType.ofOptional(id++, id++, Types.IntegerType.get(),
                                        Types.ListType.ofRequired(id++, Types.FloatType.get())))),
                        Types.NestedField.required(id++, "col_nested_2",
                                Types.MapType.ofOptional(id++, id++,
                                        Types.TimestampType.withoutZone(),
                                        Types.MapType.ofRequired(id++, id++,
                                                Types.BooleanType.get(),
                                                Types.ListType
                                                        .ofRequired(
                                                                id++,
                                                                Types.DecimalType.of(9, 3))))),
                        Types.NestedField.optional(id++, "col_nested_3", Types.StructType.of(
                                Types.NestedField.required(id++, "field_array",
                                        Types.ListType.ofRequired(id++,
                                                Types.StructType.of(Types.NestedField.optional(
                                                        id++, "nested_field_integer",
                                                        Types.IntegerType.get()),
                                                        Types.NestedField.optional(id++,
                                                                "nested_field_double",
                                                                Types.DoubleType.get())))),
                                Types.NestedField.optional(id++, "field_map",
                                        Types.MapType.ofOptional(id++, id++,
                                                Types.IntegerType.get(),
                                                Types.StringType.get())))));
        IcebergMeta.Schema proto = IcebergSchemaUtils.toProto(schema, null, null);
        Assert.assertEquals(0, proto.getPartitionKeysCount());
        {
            IcebergMeta.Field fieldInt = proto.getFields(0);
            Assert.assertEquals("col_integer", fieldInt.getName());
            IcebergMeta.Type type = fieldInt.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasIntegerType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldArrayNotNull = proto.getFields(1);
            Assert.assertEquals("col_list_not_null", fieldArrayNotNull.getName());
            IcebergMeta.Type type = fieldArrayNotNull.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasListType());
            checkOnlyOneTypeSet(type);

            IcebergMeta.Type elementType = type.getListType().getElementType();
            Assert.assertTrue(elementType.getIsNullable());
            Assert.assertTrue(elementType.hasStringType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldArrayNull = proto.getFields(2);
            Assert.assertEquals("col_list_null", fieldArrayNull.getName());
            IcebergMeta.Type type = fieldArrayNull.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasListType());
            checkOnlyOneTypeSet(type);

            IcebergMeta.Type elementType = type.getListType().getElementType();
            Assert.assertTrue(elementType.getIsNullable());
            Assert.assertTrue(elementType.hasStringType());
            checkOnlyOneTypeSet(type);
        }
        {
            IcebergMeta.Field fieldMapNotNull = proto.getFields(3);
            Assert.assertEquals("col_map_not_null", fieldMapNotNull.getName());
            IcebergMeta.Type type = fieldMapNotNull.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasMapType());
            checkOnlyOneTypeSet(type);

            IcebergMeta.Type keyType = type.getMapType().getKeyType();
            Assert.assertFalse(keyType.getIsNullable());
            Assert.assertTrue(keyType.hasIntegerType());
            checkOnlyOneTypeSet(keyType);

            IcebergMeta.Type valueType = type.getMapType().getValueType();
            Assert.assertTrue(valueType.getIsNullable());
            Assert.assertTrue(valueType.hasStringType());
            checkOnlyOneTypeSet(valueType);
        }
        {
            IcebergMeta.Field fieldMapNull = proto.getFields(4);
            Assert.assertEquals("col_map_null", fieldMapNull.getName());
            IcebergMeta.Type type = fieldMapNull.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasMapType());
            checkOnlyOneTypeSet(type);

            IcebergMeta.Type keyType = type.getMapType().getKeyType();
            Assert.assertFalse(keyType.getIsNullable());
            Assert.assertTrue(keyType.hasStringType());
            checkOnlyOneTypeSet(keyType);

            IcebergMeta.Type valueType = type.getMapType().getValueType();
            Assert.assertTrue(valueType.getIsNullable());
            Assert.assertTrue(valueType.hasDateType());
            checkOnlyOneTypeSet(valueType);
        }
        {
            IcebergMeta.Field fieldNested1 = proto.getFields(5);
            Assert.assertEquals("col_nested_1", fieldNested1.getName());
            IcebergMeta.Type type = fieldNested1.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasListType());
            checkOnlyOneTypeSet(type);

            IcebergMeta.Type elementType = type.getListType().getElementType();
            Assert.assertFalse(elementType.getIsNullable());
            Assert.assertTrue(elementType.hasMapType());
            checkOnlyOneTypeSet(elementType);

            IcebergMeta.Type nestedKeyType = elementType.getMapType().getKeyType();
            Assert.assertFalse(nestedKeyType.getIsNullable());
            Assert.assertTrue(nestedKeyType.hasIntegerType());
            checkOnlyOneTypeSet(nestedKeyType);

            IcebergMeta.Type nestedValueType = elementType.getMapType().getValueType();
            Assert.assertTrue(nestedValueType.getIsNullable());
            Assert.assertTrue(nestedValueType.hasListType());
            checkOnlyOneTypeSet(nestedValueType);

            IcebergMeta.Type nestedNestedElementType =
                    nestedValueType.getListType().getElementType();
            Assert.assertFalse(nestedNestedElementType.getIsNullable());
            Assert.assertTrue(nestedNestedElementType.hasFloatType());
            checkOnlyOneTypeSet(nestedNestedElementType);
        }
        {
            IcebergMeta.Field fieldNested2 = proto.getFields(6);
            Assert.assertEquals("col_nested_2", fieldNested2.getName());
            IcebergMeta.Type type = fieldNested2.getType();
            Assert.assertFalse(type.getIsNullable());
            Assert.assertTrue(type.hasMapType());
            checkOnlyOneTypeSet(type);

            IcebergMeta.Type keyType = type.getMapType().getKeyType();
            Assert.assertFalse(keyType.getIsNullable());
            Assert.assertTrue(keyType.hasTimestampType());
            checkOnlyOneTypeSet(keyType);

            IcebergMeta.Type valueType = type.getMapType().getValueType();
            Assert.assertTrue(valueType.getIsNullable());
            Assert.assertTrue(valueType.hasMapType());
            checkOnlyOneTypeSet(valueType);

            IcebergMeta.Type nestedKeyType = valueType.getMapType().getKeyType();
            Assert.assertFalse(nestedKeyType.getIsNullable());
            Assert.assertTrue(nestedKeyType.hasBooleanType());
            checkOnlyOneTypeSet(nestedKeyType);

            IcebergMeta.Type nestedValueType = valueType.getMapType().getValueType();
            Assert.assertFalse(nestedValueType.getIsNullable());
            Assert.assertTrue(nestedValueType.hasListType());
            checkOnlyOneTypeSet(nestedValueType);

            IcebergMeta.Type nestedNestedElementType =
                    nestedValueType.getListType().getElementType();
            Assert.assertFalse(nestedNestedElementType.getIsNullable());
            Assert.assertTrue(nestedNestedElementType.hasDecimalType());
            checkOnlyOneTypeSet(nestedNestedElementType);
        }
        {
            IcebergMeta.Field fieldNested3 = proto.getFields(7);
            Assert.assertEquals("col_nested_3", fieldNested3.getName());
            IcebergMeta.Type type = fieldNested3.getType();
            Assert.assertTrue(type.getIsNullable());
            Assert.assertTrue(type.hasStructType());
            checkOnlyOneTypeSet(type);

            IcebergMeta.Type fieldType1 = type.getStructType().getFields(0).getType();
            Assert.assertFalse(fieldType1.getIsNullable());
            Assert.assertTrue(fieldType1.hasListType());
            checkOnlyOneTypeSet(fieldType1);

            IcebergMeta.Type nestedRowType = fieldType1.getListType().getElementType();
            Assert.assertFalse(nestedRowType.getIsNullable());
            Assert.assertTrue(nestedRowType.hasStructType());
            checkOnlyOneTypeSet(nestedRowType);

            IcebergMeta.Type nestedFieldType1 =
                    nestedRowType.getStructType().getFields(0).getType();
            Assert.assertTrue(nestedFieldType1.getIsNullable());
            Assert.assertTrue(nestedFieldType1.hasIntegerType());
            checkOnlyOneTypeSet(nestedFieldType1);

            IcebergMeta.Type nestedFieldType2 =
                    nestedRowType.getStructType().getFields(1).getType();
            Assert.assertTrue(nestedFieldType2.getIsNullable());
            Assert.assertTrue(nestedFieldType2.hasDoubleType());
            checkOnlyOneTypeSet(nestedFieldType2);

            IcebergMeta.Type fieldType2 = type.getStructType().getFields(1).getType();
            Assert.assertTrue(fieldType2.getIsNullable());
            Assert.assertTrue(fieldType2.hasMapType());
            checkOnlyOneTypeSet(fieldType2);

            IcebergMeta.Type nestedKeyType = fieldType2.getMapType().getKeyType();
            Assert.assertFalse(nestedKeyType.getIsNullable());
            Assert.assertTrue(nestedKeyType.hasIntegerType());
            checkOnlyOneTypeSet(nestedKeyType);

            IcebergMeta.Type nestedValueType = fieldType2.getMapType().getValueType();
            Assert.assertTrue(nestedValueType.getIsNullable());
            Assert.assertTrue(nestedValueType.hasStringType());
            checkOnlyOneTypeSet(nestedValueType);
        }
    }
}
