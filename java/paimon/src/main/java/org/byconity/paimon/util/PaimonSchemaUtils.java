package org.byconity.paimon.util;

import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.*;
import org.byconity.proto.PaimonMeta;

import java.util.List;
import java.util.Map;

public abstract class PaimonSchemaUtils {

    public static PaimonMeta.Schema toProto(Schema schema) {
        return toProto(schema.fields(), schema.primaryKeys(), schema.partitionKeys(),
                schema.options());
    }

    public static PaimonMeta.Schema toProto(List<DataField> fields, List<String> primaryKeys,
            List<String> partitionKeys, Map<String, String> options) {
        Converter converter = new Converter(fields, primaryKeys, partitionKeys, options);
        return converter.transform();
    }

    private static final class Converter {
        private final List<DataField> fields;
        private final List<String> primaryKeys;
        private final List<String> partitionKeys;
        private final Map<String, String> options;
        private final PaimonMeta.Schema.Builder builder = PaimonMeta.Schema.newBuilder();

        public Converter(List<DataField> fields, List<String> primaryKeys,
                List<String> partitionKeys, Map<String, String> options) {
            this.fields = fields;
            this.primaryKeys = primaryKeys;
            this.partitionKeys = partitionKeys;
            this.options = options;
        }

        private PaimonMeta.Schema transform() {
            transformFields();
            transformPrimaryKeys();
            transformPartitionKeys();
            transformOptions();
            return builder.build();
        }

        private void transformFields() {
            for (DataField field : fields) {
                transformField(field);
            }
        }

        private void transformField(DataField field) {
            String name = field.name();
            DataType type = field.type();

            PaimonMeta.Type.Builder typeBuilder = PaimonMeta.Type.newBuilder();
            transformFieldType(type, typeBuilder);

            builder.addFields(
                    PaimonMeta.Field.newBuilder().setName(name).setType(typeBuilder).build());
        }

        private void transformFieldType(DataType type, PaimonMeta.Type.Builder typeBuilder) {
            typeBuilder.setIsNullable(type.isNullable());
            switch (type.getTypeRoot()) {
                case ARRAY: {
                    PaimonMeta.Type.Builder elementTypeBuilder = PaimonMeta.Type.newBuilder();
                    transformFieldType(((ArrayType) type).getElementType(), elementTypeBuilder);

                    typeBuilder.setArrayType(PaimonMeta.ArrayType.newBuilder()
                            .setElementType(elementTypeBuilder.build()).build());
                    break;
                }
                case BIGINT: {
                    typeBuilder.setBigIntType(PaimonMeta.BigIntType.newBuilder());
                    break;
                }
                case BINARY: {
                    typeBuilder.setBinaryType(PaimonMeta.BinaryType.newBuilder()
                            .setLength(((BinaryType) type).getLength()).build());
                    break;
                }
                case BOOLEAN: {
                    typeBuilder.setBooleanType(PaimonMeta.BooleanType.newBuilder().build());
                    break;
                }
                case CHAR: {
                    typeBuilder.setCharType(PaimonMeta.CharType.newBuilder()
                            .setLength(((CharType) type).getLength()).build());
                    break;
                }
                case DATE: {
                    typeBuilder.setDateType(PaimonMeta.DateType.newBuilder().build());
                    break;
                }
                case DECIMAL: {
                    typeBuilder.setDecimalType(PaimonMeta.DecimalType.newBuilder()
                            .setPrecision(((DecimalType) type).getPrecision())
                            .setScale(((DecimalType) type).getScale()).build());
                    break;
                }
                case DOUBLE: {
                    typeBuilder.setDoubleType(PaimonMeta.DoubleType.newBuilder().build());
                    break;
                }
                case FLOAT: {
                    typeBuilder.setFloatType(PaimonMeta.FloatType.newBuilder().build());
                    break;
                }
                case INTEGER: {
                    typeBuilder.setIntType(PaimonMeta.IntType.newBuilder().build());
                    break;
                }
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                    typeBuilder.setLocalZonedTimestampType(PaimonMeta.LocalZonedTimestampType
                            .newBuilder()
                            .setPrecision(((LocalZonedTimestampType) type).getPrecision()).build());
                    break;
                }
                case MAP: {
                    PaimonMeta.Type.Builder keyTypeBuilder = PaimonMeta.Type.newBuilder();
                    PaimonMeta.Type.Builder valueTypeBuilder = PaimonMeta.Type.newBuilder();
                    transformFieldType(((MapType) type).getKeyType(), keyTypeBuilder);
                    transformFieldType(((MapType) type).getValueType(), valueTypeBuilder);

                    typeBuilder.setMapType(
                            PaimonMeta.MapType.newBuilder().setKeyType(keyTypeBuilder.build())
                                    .setValueType(valueTypeBuilder.build()).build());
                    break;
                }
                case ROW: {
                    PaimonMeta.RowType.Builder rowTypeBuilder = PaimonMeta.RowType.newBuilder();
                    for (DataField field : ((RowType) type).getFields()) {
                        PaimonMeta.Type.Builder fieldTypeBuilder = PaimonMeta.Type.newBuilder();
                        transformFieldType(field.type(), fieldTypeBuilder);
                        rowTypeBuilder.addFields(PaimonMeta.Field.newBuilder().setName(field.name())
                                .setType(fieldTypeBuilder.build()).build());
                    }
                    typeBuilder.setRowType(rowTypeBuilder.build());
                    break;
                }
                case SMALLINT: {
                    typeBuilder.setSmallIntType(PaimonMeta.SmallIntType.newBuilder().build());
                    break;
                }
                case TIMESTAMP_WITHOUT_TIME_ZONE: {
                    typeBuilder.setTimestampType(PaimonMeta.TimestampType.newBuilder()
                            .setPrecision(((TimestampType) type).getPrecision()).build());
                    break;
                }
                case TINYINT: {
                    typeBuilder.setTinyIntType(PaimonMeta.TinyIntType.newBuilder().build());
                    break;
                }
                case VARBINARY: {
                    typeBuilder.setVarBinaryType(PaimonMeta.VarBinaryType.newBuilder()
                            .setLength(((VarBinaryType) type).getLength()).build());
                    break;
                }
                case VARCHAR: {
                    typeBuilder.setVarCharType(PaimonMeta.VarCharType.newBuilder()
                            .setLength(((VarCharType) type).getLength()).build());
                    break;
                }
                default:
                    throw new UnsupportedOperationException();
            }
        }

        private void transformPrimaryKeys() {
            for (String primaryKey : primaryKeys) {
                builder.addPrimaryKeys(primaryKey);
            }
        }

        private void transformPartitionKeys() {
            for (String partitionKey : partitionKeys) {
                builder.addPartitionKeys(partitionKey);
            }
        }

        private void transformOptions() {
            for (Map.Entry<String, String> entry : options.entrySet()) {
                builder.addOptions(PaimonMeta.Option.newBuilder().setName(entry.getKey())
                        .setValue(entry.getValue()).build());
            }
        }
    }
}
