package org.byconity.iceberg.util;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.byconity.proto.IcebergMeta;

import java.util.Map;

public abstract class IcebergSchemaUtils {

    public static IcebergMeta.Schema toProto(Schema schema, PartitionSpec spec,
            Map<String, String> properties) {
        Converter converter = new Converter(schema, spec, properties);
        return converter.transform();
    }

    private static final class Converter {
        private final Schema schema;
        private final PartitionSpec spec;
        private final Map<String, String> properties;

        private final IcebergMeta.Schema.Builder builder = IcebergMeta.Schema.newBuilder();

        private Converter(Schema schema, PartitionSpec spec, Map<String, String> properties) {
            this.schema = schema;
            this.spec = spec;
            this.properties = properties;
        }

        private IcebergMeta.Schema transform() {
            transformFields();
            transformPartitionKeys();
            transformOptions();
            return builder.build();
        }

        private void transformFields() {
            for (Types.NestedField field : schema.columns()) {
                transformField(field);
            }
        }

        private void transformField(Types.NestedField field) {
            int id = field.fieldId();
            String name = field.name();

            IcebergMeta.Type.Builder typeBuilder = IcebergMeta.Type.newBuilder();
            transformFieldType(field, typeBuilder);

            builder.addFields(IcebergMeta.Field.newBuilder().setId(id).setName(name)
                    .setType(typeBuilder).build());
        }

        private void transformFieldType(Types.NestedField field,
                IcebergMeta.Type.Builder typeBuilder) {
            typeBuilder.setIsNullable(field.isOptional());
            switch (field.type().typeId()) {
                case BOOLEAN: {
                    typeBuilder.setBooleanType(IcebergMeta.BooleanType.newBuilder().build());
                    break;
                }
                case INTEGER: {
                    typeBuilder.setIntegerType(IcebergMeta.IntegerType.newBuilder().build());
                    break;
                }
                case LONG: {
                    typeBuilder.setLongType(IcebergMeta.LongType.newBuilder());
                    break;
                }
                case FLOAT: {
                    typeBuilder.setFloatType(IcebergMeta.FloatType.newBuilder().build());
                    break;
                }
                case DOUBLE: {
                    typeBuilder.setDoubleType(IcebergMeta.DoubleType.newBuilder().build());
                    break;
                }
                case DATE: {
                    typeBuilder.setDateType(IcebergMeta.DateType.newBuilder().build());
                    break;
                }
                case TIMESTAMP: {
                    typeBuilder.setTimestampType(IcebergMeta.TimestampType.newBuilder().build());
                    break;
                }
                case STRING: {
                    typeBuilder.setStringType(IcebergMeta.StringType.newBuilder().build());
                    break;
                }
                case UUID: {
                    typeBuilder.setUUIDType(IcebergMeta.UUIDType.newBuilder().build());
                    break;
                }
                case FIXED: {
                    typeBuilder.setFixedType(IcebergMeta.FixedType.newBuilder()
                            .setLength(((Types.FixedType) field.type()).length()).build());
                    break;
                }
                case BINARY: {
                    typeBuilder.setBinaryType(IcebergMeta.BinaryType.newBuilder().build());
                    break;
                }
                case DECIMAL: {
                    Types.DecimalType decimalType = (Types.DecimalType) field.type();
                    typeBuilder.setDecimalType(IcebergMeta.DecimalType.newBuilder()
                            .setPrecision(decimalType.precision()).setScale(decimalType.scale())
                            .build());
                    break;
                }
                case LIST: {
                    IcebergMeta.Type.Builder elementTypeBuilder = IcebergMeta.Type.newBuilder();
                    Types.ListType listType = field.type().asListType();
                    transformFieldType(listType.field(listType.elementId()), elementTypeBuilder);

                    typeBuilder.setListType(IcebergMeta.ListType.newBuilder()
                            .setElementType(elementTypeBuilder.build()).build());
                    break;
                }
                case MAP: {
                    IcebergMeta.Type.Builder keyTypeBuilder = IcebergMeta.Type.newBuilder();
                    IcebergMeta.Type.Builder valueTypeBuilder = IcebergMeta.Type.newBuilder();
                    Types.MapType mapType = field.type().asMapType();
                    transformFieldType(mapType.field(mapType.keyId()), keyTypeBuilder);
                    transformFieldType(mapType.field(mapType.valueId()), valueTypeBuilder);

                    typeBuilder.setMapType(
                            IcebergMeta.MapType.newBuilder().setKeyType(keyTypeBuilder.build())
                                    .setValueType(valueTypeBuilder.build()).build());
                    break;
                }
                case STRUCT: {
                    IcebergMeta.StructType.Builder structTypeBuilder =
                            IcebergMeta.StructType.newBuilder();
                    for (Types.NestedField nestedField : field.type().asStructType().fields()) {
                        IcebergMeta.Type.Builder fieldTypeBuilder = IcebergMeta.Type.newBuilder();
                        transformFieldType(nestedField, fieldTypeBuilder);
                        structTypeBuilder.addFields(IcebergMeta.Field.newBuilder()
                                .setId(nestedField.fieldId()).setName(nestedField.name())
                                .setType(fieldTypeBuilder.build()).build());
                    }
                    typeBuilder.setStructType(structTypeBuilder.build());
                    break;
                }
                default:
                    throw new UnsupportedOperationException(
                            String.format("Unsupported type: %s", field.type().typeId()));
            }
        }

        private void transformPartitionKeys() {
            if (spec == null) {
                return;
            }
            for (PartitionField partitionField : spec.fields()) {
                builder.addPartitionKeys(partitionField.name());
            }
        }

        private void transformOptions() {
            if (properties == null) {
                return;
            }
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                builder.addProperties(IcebergMeta.Property.newBuilder().setName(entry.getKey())
                        .setValue(entry.getValue()).build());
            }
        }
    }
}
