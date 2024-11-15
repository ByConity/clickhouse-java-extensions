package org.byconity.iceberg.cli;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class CreateTableProcessor extends ActionProcessor {
    @SuppressWarnings("all")
    private static final class CreateTableParams {
        @SerializedName("database")
        private String database;
        @SerializedName("table")
        private String table;
        @SerializedName("columns")
        private List<Column> columns;
        @SerializedName("partitionKeys")
        private List<String> partitionKeys;
        @SerializedName("schemaProperties")
        private Map<String, String> schemaProperties;
    }


    @SuppressWarnings("all")
    private static final class Column {
        @SerializedName("name")
        private String name;
        @SerializedName("type")
        private String type;
    }


    private final CreateTableParams params;
    private Boolean isSpecifiedFieldId;
    private int idGenerator = 0;

    public static void process(Catalog catalog, String arg) throws Exception {
        CreateTableParams params;
        try {
            params = GSON.fromJson(arg, CreateTableParams.class);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Wrong arg format for action '%s', it should be json.",
                            ActionType.CREATE_TABLE.name()));
        }
        new CreateTableProcessor(catalog, params).doProcess();
    }

    private CreateTableProcessor(Catalog catalog, CreateTableParams params) {
        super(catalog);
        this.params = params;
    }

    @Override
    protected void doProcess() throws Exception {
        TableIdentifier identifier = TableIdentifier.of(params.database, params.table);

        Namespace namespace = Namespace.of(params.database);
        Preconditions.checkState(((SupportsNamespaces) catalog).namespaceExists(namespace),
                String.format("Database '%s' not exists.", params.database));
        Preconditions.checkState(!catalog.tableExists(identifier),
                String.format("Table '%s' already exists.", params.table));

        List<Types.NestedField> nestedFields = Lists.newArrayList();
        // field id can be automatically generated or specified by user
        for (Column column : params.columns) {
            nestedFields.add(parseSchemaType(column));
        }
        Schema schema = new Schema(nestedFields);

        PartitionSpec partitionSpec = null;
        if (CollectionUtils.isNotEmpty(params.partitionKeys)) {
            PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(schema);
            for (String partitionKey : params.partitionKeys) {
                partitionSpecBuilder.identity(partitionKey);
            }
            partitionSpec = partitionSpecBuilder.build();
        }

        Map<String, String> properties = Maps.newHashMap();
        if (MapUtils.isNotEmpty(params.schemaProperties)) {
            properties.putAll(params.schemaProperties);
        }

        catalog.createTable(identifier, schema, partitionSpec, properties);

        System.out.printf("Create table '%s.%s' successfully.\n", params.database, params.table);
    }

    private Types.NestedField parseSchemaType(Column column) throws Exception {
        String type = column.type.toLowerCase().replaceAll("\\s", "");
        TypeInfo typeInfo = parseTypeInfo(type);
        return Types.NestedField.of(typeInfo.id, typeInfo.nullable, column.name, typeInfo.type);
    }

    private TypeInfo parseTypeInfo(String type) throws Exception {
        // If type starts with id: like 1:Int
        final int id;
        if (Character.isDigit(type.charAt(0))) {
            Preconditions.checkState(isSpecifiedFieldId == null || isSpecifiedFieldId,
                    "All fields must either include 'id' or none of them should include it");

            int i = 0;
            while (i < type.length() && Character.isDigit(type.charAt(i))) {
                i++;
            }
            Preconditions.checkState(i < type.length() && type.charAt(i) == ':');
            id = Integer.parseInt(type.substring(0, i));
            type = type.substring(i + 1);
            isSpecifiedFieldId = true;
        } else {
            Preconditions.checkState(isSpecifiedFieldId == null || !isSpecifiedFieldId,
                    "All fields must either include 'id' or none of them should include it");

            id = idGenerator++;
            isSpecifiedFieldId = false;
        }

        final boolean nullable;
        String prefix = "nullable(";
        if (type.startsWith(prefix)) {
            type = type.substring(prefix.length(), type.length() - 1);
            nullable = true;
        } else {
            nullable = false;
        }

        BiFunction<String, String, String> getParams =
                (content, starts) -> content.substring(starts.length() + 1, content.length() - 1);
        if (type.equals("boolean")) {
            return new TypeInfo(id, Types.BooleanType.get(), nullable);
        } else if (type.equals("integer")) {
            return new TypeInfo(id, Types.IntegerType.get(), nullable);
        } else if (type.equals("long")) {
            return new TypeInfo(id, Types.LongType.get(), nullable);
        } else if (type.equals("float")) {
            return new TypeInfo(id, Types.FloatType.get(), nullable);
        } else if (type.equals("double")) {
            return new TypeInfo(id, Types.DoubleType.get(), nullable);
        } else if (type.equals("date")) {
            return new TypeInfo(id, Types.DateType.get(), nullable);
        } else if (type.equals("timestamp")) {
            return new TypeInfo(id, Types.TimestampType.withoutZone(), nullable);
        } else if (type.equals("string")) {
            return new TypeInfo(id, Types.StringType.get(), nullable);
        } else if (type.equals("uuid")) {
            return new TypeInfo(id, Types.UUIDType.get(), nullable);
        } else if (type.startsWith("fixed(")) {
            int length = Integer.parseInt(getParams.apply(type, "fixed"));
            return new TypeInfo(id, Types.FixedType.ofLength(length), nullable);
        } else if (type.equals("binary")) {
            return new TypeInfo(id, Types.BinaryType.get(), nullable);
        } else if (type.startsWith("decimal(")) {
            String[] segments = getParams.apply(type, "decimal").split(",");
            int precision = Integer.parseInt(segments[0]);
            int scale = Integer.parseInt(segments[1]);
            return new TypeInfo(id, Types.DecimalType.of(precision, scale), nullable);
        } else if (type.startsWith("list<")) {
            String elementType = getParams.apply(type, "list");
            TypeInfo elementTypeInfo = parseTypeInfo(elementType);
            if (elementTypeInfo.nullable) {
                return new TypeInfo(id,
                        Types.ListType.ofOptional(elementTypeInfo.id, elementTypeInfo.type),
                        nullable);
            } else {
                return new TypeInfo(id,
                        Types.ListType.ofRequired(elementTypeInfo.id, elementTypeInfo.type),
                        nullable);
            }
        } else if (type.startsWith("map<")) {
            String keyAndValueType = getParams.apply(type, "map");
            int smallParenthesesCnt = 0;
            int angleBracketCnt = 0;
            int commaIndex = -1;
            for (int i = 0; i < keyAndValueType.length(); i++) {
                char c = keyAndValueType.charAt(i);
                if (c == '(') {
                    smallParenthesesCnt++;
                } else if (c == ')') {
                    smallParenthesesCnt--;
                } else if (c == '<') {
                    angleBracketCnt++;
                } else if (c == '>') {
                    angleBracketCnt--;
                } else if (c == ',') {
                    if (smallParenthesesCnt == 0 && angleBracketCnt == 0) {
                        commaIndex = i;
                        break;
                    }
                }
            }
            String keyType = keyAndValueType.substring(0, commaIndex);
            String valueType = keyAndValueType.substring(commaIndex + 1);
            TypeInfo keyTypeInfo = parseTypeInfo(keyType);
            TypeInfo valueTypeInfo = parseTypeInfo(valueType);

            Preconditions.checkState(!keyTypeInfo.nullable, "Map's key type must be non-nullable");
            if (valueTypeInfo.nullable) {
                return new TypeInfo(id, Types.MapType.ofOptional(keyTypeInfo.id, valueTypeInfo.id,
                        keyTypeInfo.type, valueTypeInfo.type), nullable);
            } else {
                return new TypeInfo(id, Types.MapType.ofRequired(keyTypeInfo.id, valueTypeInfo.id,
                        keyTypeInfo.type, valueTypeInfo.type), nullable);
            }
        } else if (type.startsWith("struct<")) {
            String fieldNameAndTypes = getParams.apply(type, "struct");
            List<Types.NestedField> fields = Lists.newArrayList();
            int i = 0;
            while (i < fieldNameAndTypes.length()) {
                int start = i;
                int smallParenthesesCnt = 0;
                int angleBracketCnt = 0;
                int verticalIndex = -1;
                int commaIndex = -1;
                for (; i < fieldNameAndTypes.length(); i++) {
                    char c = fieldNameAndTypes.charAt(i);
                    if (c == '(') {
                        smallParenthesesCnt++;
                    } else if (c == ')') {
                        smallParenthesesCnt--;
                    } else if (c == '<') {
                        angleBracketCnt++;
                    } else if (c == '>') {
                        angleBracketCnt--;
                    } else if (c == ',') {
                        if (smallParenthesesCnt == 0 && angleBracketCnt == 0) {
                            commaIndex = i;
                            ++i;
                            break;
                        }
                    } else if (c == '|') {
                        if (smallParenthesesCnt == 0 && angleBracketCnt == 0) {
                            verticalIndex = i;
                        }
                    }
                }

                String fieldName = fieldNameAndTypes.substring(start, verticalIndex);
                String fieldType;
                if (commaIndex == -1) {
                    fieldType = fieldNameAndTypes.substring(verticalIndex + 1);
                } else {
                    fieldType = fieldNameAndTypes.substring(verticalIndex + 1, commaIndex);
                }

                TypeInfo fieldTypeInfo = parseTypeInfo(fieldType);

                fields.add(Types.NestedField.of(fieldTypeInfo.id, fieldTypeInfo.nullable, fieldName,
                        fieldTypeInfo.type));
            }
            return new TypeInfo(id, Types.StructType.of(fields), nullable);
        } else {
            throw new UnsupportedOperationException(String.format("Unsupported type: %s.", type));
        }
    }

    private static final class TypeInfo {
        private final int id;
        private final Type type;
        private final boolean nullable;

        public TypeInfo(int id, Type type, boolean nullable) {
            this.id = id;
            this.type = type;
            this.nullable = nullable;
        }
    }
}
