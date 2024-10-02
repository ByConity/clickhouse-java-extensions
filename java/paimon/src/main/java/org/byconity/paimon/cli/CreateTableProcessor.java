package org.byconity.paimon.cli;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.MapUtils;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.*;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

public class CreateTableProcessor extends ActionProcessor {

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

    private final CreateTableParams params;

    private CreateTableProcessor(Catalog catalog, CreateTableParams params) {
        super(catalog);
        this.params = params;
    }

    @Override
    protected void doProcess() throws Exception {
        Identifier identifier = Identifier.create(params.database, params.table);

        Preconditions.checkState(catalog.databaseExists(params.database),
                String.format("Database '%s' not exists.", params.database));
        Preconditions.checkState(!catalog.tableExists(identifier),
                String.format("Table '%s' already exists.", params.table));

        Schema.Builder schemaBuilder = Schema.newBuilder();
        if (params.primaryKeys != null) {
            schemaBuilder.primaryKey(params.primaryKeys);
        }
        if (params.partitionKeys != null) {
            schemaBuilder.partitionKeys(params.partitionKeys);
        }

        for (Column column : params.columns) {
            schemaBuilder.column(column.name, parseTypeIgnoreCaseAndBlank(column.type));
        }

        if (MapUtils.isNotEmpty(params.schemaOptions)) {
            schemaBuilder.options(params.schemaOptions);
        }

        Schema schema = schemaBuilder.build();
        catalog.createTable(identifier, schema, false);

        System.out.printf("Create table '%s.%s' successfully.\n", params.database, params.table);
    }

    private DataType parseTypeIgnoreCaseAndBlank(String type) throws Exception {
        return parseType(type.toLowerCase().replaceAll("\\s", ""));
    }

    private DataType parseType(String type) throws Exception {
        boolean nullable = false;
        String prefix = "nullable(";
        if (type.startsWith(prefix)) {
            type = type.substring(prefix.length(), type.length() - 1);
            nullable = true;
        }
        TypeInfo typeInfo;

        UnaryOperator<String> getSingleParam =
                (s) -> s.substring(s.indexOf("(") + 1, s.indexOf(")"));
        if ((typeInfo = PRIMITIVE_GROUP.match(type)) != null) {
            return typeInfo.constructor.newInstance(nullable);
        } else if ((typeInfo = DATE_GROUP.match(type)) != null) {
            return typeInfo.constructor.newInstance(nullable);
        } else if ((typeInfo = TIME_GROUP.match(type)) != null) {
            int precision;
            if (type.contains("(")) {
                precision = Integer.parseInt(getSingleParam.apply(type));
            } else {
                if (TimeType.class.equals(typeInfo.clazz)) {
                    precision = TimeType.DEFAULT_PRECISION;
                } else if (TimestampType.class.equals(typeInfo.clazz)) {
                    precision = TimestampType.DEFAULT_PRECISION;
                } else {
                    Preconditions.checkState(LocalZonedTimestampType.class.equals(typeInfo.clazz));
                    precision = TimestampType.DEFAULT_PRECISION;
                }
            }
            return typeInfo.constructor.newInstance(nullable, precision);
        } else if ((typeInfo = LENGTH_GROUP.match(type)) != null) {
            int length = Integer.parseInt(getSingleParam.apply(type));
            return typeInfo.constructor.newInstance(nullable, length);
        } else if ((typeInfo = DECIMAL_GROUP.match(type)) != null) {
            String[] segments = removeBoundParentheses(typeInfo, type).split(",");
            int precision = Integer.parseInt(segments[0]);
            int scale = Integer.parseInt(segments[1]);
            return typeInfo.constructor.newInstance(nullable, precision, scale);
        } else if ((typeInfo = ARRAY_GROUP.match(type)) != null) {
            String elementType = removeBoundParentheses(typeInfo, type);
            return typeInfo.constructor.newInstance(nullable, parseType(elementType));
        } else if ((typeInfo = MAP_GROUP.match(type)) != null) {
            String keyAndValueType = removeBoundParentheses(typeInfo, type);
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
            return typeInfo.constructor.newInstance(nullable, parseType(keyType),
                    parseType(valueType));
        } else if ((typeInfo = ROW_GROUP.match(type)) != null) {
            String fieldNameAndTypes = removeBoundParentheses(typeInfo, type);
            List<DataField> fields = Lists.newArrayList();
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
                fields.add(new DataField(fields.size(), fieldName, parseType(fieldType)));
            }
            return typeInfo.constructor.newInstance(nullable, fields);
        } else {
            throw new UnsupportedOperationException(String.format("Unsupported type: %s.", type));
        }
    }

    private static String removeBoundParentheses(TypeInfo typeInfo, String type) {
        return type.substring(typeInfo.name.length() + 1, type.length() - 1);
    }

    private static final TypeGroupInfo PRIMITIVE_GROUP = new TypeGroupInfo(false, false,
            Arrays.asList(createTypeInfo(BooleanType.class), createTypeInfo(TinyIntType.class),
                    createTypeInfo(SmallIntType.class), createTypeInfo(IntType.class),
                    createTypeInfo(BigIntType.class), createTypeInfo(FloatType.class),
                    createTypeInfo(DoubleType.class)));
    private static final TypeGroupInfo DATE_GROUP = new TypeGroupInfo(false, false,
            Collections.singletonList(createTypeInfo(DateType.class)));
    private static final TypeGroupInfo TIME_GROUP = new TypeGroupInfo(false, false,
            Arrays.asList(createTypeInfo(TimeType.class), createTypeInfo(TimestampType.class),
                    createTypeInfo(LocalZonedTimestampType.class)));
    private static final TypeGroupInfo LENGTH_GROUP = new TypeGroupInfo(true, false,
            Arrays.asList(createTypeInfo(CharType.class), createTypeInfo(VarCharType.class),
                    createTypeInfo(VarBinaryType.class), createTypeInfo(BinaryType.class)));
    private static final TypeGroupInfo DECIMAL_GROUP = new TypeGroupInfo(true, false,
            Collections.singletonList(createTypeInfo(DecimalType.class)));
    private static final TypeGroupInfo ARRAY_GROUP = new TypeGroupInfo(false, true,
            Collections.singletonList(createTypeInfo(ArrayType.class)));
    private static final TypeGroupInfo MAP_GROUP = new TypeGroupInfo(false, true,
            Collections.singletonList(createTypeInfo(MapType.class)));
    private static final TypeGroupInfo ROW_GROUP = new TypeGroupInfo(false, true,
            Collections.singletonList(createTypeInfo(RowType.class)));

    private static TypeInfo createTypeInfo(Class<? extends DataType> clazz) {
        String name = clazz.getSimpleName();
        name = name.toLowerCase();
        name = name.substring(0, name.length() - "type".length());
        return new TypeInfo(clazz, name);
    }

    static {
        try {
            for (TypeInfo type : PRIMITIVE_GROUP.types) {
                type.constructor = type.clazz.getConstructor(boolean.class);
            }
            for (TypeInfo type : DATE_GROUP.types) {
                type.constructor = type.clazz.getConstructor(boolean.class);
            }
            for (TypeInfo type : TIME_GROUP.types) {
                type.constructor = type.clazz.getConstructor(boolean.class, int.class);
            }
            for (TypeInfo type : LENGTH_GROUP.types) {
                type.constructor = type.clazz.getConstructor(boolean.class, int.class);
            }
            for (TypeInfo type : DECIMAL_GROUP.types) {
                type.constructor = type.clazz.getConstructor(boolean.class, int.class, int.class);
            }
            for (TypeInfo type : ARRAY_GROUP.types) {
                type.constructor = type.clazz.getConstructor(boolean.class, DataType.class);
            }
            for (TypeInfo type : MAP_GROUP.types) {
                type.constructor =
                        type.clazz.getConstructor(boolean.class, DataType.class, DataType.class);
            }
            for (TypeInfo type : ROW_GROUP.types) {
                type.constructor = type.clazz.getConstructor(boolean.class, List.class);
            }
        } catch (Throwable e) {
            throw new Error(e);
        }
    }


    private static final class TypeInfo {
        private final Class<? extends DataType> clazz;
        private final String name;
        private Constructor<? extends DataType> constructor;

        private TypeInfo(Class<? extends DataType> clazz, String name) {
            this.clazz = clazz;
            this.name = name;
        }
    }


    private static final class TypeGroupInfo {
        private final boolean hasArgs;
        private final boolean isNested;
        private final List<TypeInfo> types;

        public TypeGroupInfo(boolean hasArgs, boolean isNested, List<TypeInfo> types) {
            this.hasArgs = hasArgs;
            this.isNested = isNested;
            this.types = types;
        }

        private TypeInfo match(String type) {
            for (TypeInfo typeInfo : types) {
                if (!hasArgs && !isNested) {
                    if (type.equals(typeInfo.name)) {
                        return typeInfo;
                    }
                } else if (hasArgs) {
                    if (type.startsWith(typeInfo.name + "(")) {
                        return typeInfo;
                    }
                } else {
                    if (type.startsWith(typeInfo.name + "<")) {
                        return typeInfo;
                    }
                }
            }
            return null;
        }
    }


    private static final class CreateTableParams {
        @SerializedName("database")
        private String database;
        @SerializedName("table")
        private String table;
        @SerializedName("columns")
        private List<Column> columns;
        @SerializedName("primaryKeys")
        private List<String> primaryKeys;
        @SerializedName("partitionKeys")
        private List<String> partitionKeys;
        @SerializedName("schemaOptions")
        private Map<String, String> schemaOptions;
    }


    private static final class Column {
        @SerializedName("name")
        private String name;
        @SerializedName("type")
        private String type;
    }

}
