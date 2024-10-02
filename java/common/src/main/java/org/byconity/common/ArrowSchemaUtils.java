package org.byconity.common;

import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.lang.model.type.ArrayType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Refer to the type conversion in spark
 * https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/util/ArrowUtils.scala
 */
public class ArrowSchemaUtils {
    private static final String ORIGINAL_TYPE = "originalType";
    private static final String MAP_TYPE = "mapType";

    private ArrowSchemaUtils() {}

    public static Schema toArrowSchema(ColumnType[] columnTypes, String[] columnNames) {
        int len = columnTypes.length;
        List<Field> fields = new ArrayList<>();

        for (int i = 0; i < len; i++) {
            Field field = toArrowField(columnTypes[i], columnNames[i], true);
            fields.add(field);
        }
        return new Schema(fields);
    }

    public static Field toArrowField(ColumnType columnType, String columnName, boolean nullable) {
        final ArrowType arrowType;
        List<Field> children = null;
        Map<String, String> metadata = null;

        switch (columnType.getTypeValue()) {
            case TINYINT:
            case BYTE:
                arrowType = new ArrowType.Int(Byte.SIZE, true);
                break;
            case BOOLEAN:
                arrowType = ArrowType.Bool.INSTANCE;
                break;
            case SHORT:
                arrowType = new ArrowType.Int(Short.SIZE, true);
                break;
            case INT:
                arrowType = new ArrowType.Int(Integer.SIZE, true);
                break;
            case LONG:
                arrowType = new ArrowType.Int(Long.SIZE, true);
                break;
            case FLOAT:
                arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
                break;
            case DOUBLE:
                arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
                break;
            case BINARY:
                arrowType = ArrowType.Binary.INSTANCE;
                break;
            case STRING:
                arrowType = ArrowType.Utf8.INSTANCE;
                break;
            case DATE:
                arrowType = new ArrowType.Date(DateUnit.DAY);
                break;
            case DATETIME:
            case DATETIME_MICROS:
                arrowType = new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
                break;
            case DATETIME_MILLIS:
                arrowType = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
                break;
            case DECIMAL64:
                arrowType = new ArrowType.Decimal(columnType.precision, columnType.scale, 64);
                break;
            case DECIMAL128:
                arrowType = new ArrowType.Decimal(columnType.precision, columnType.scale, 128);
                break;
            case ARRAY: {
                arrowType = ArrowType.List.INSTANCE;
                List<String> childNames = columnType.getChildNames();
                List<ColumnType> childTypes = columnType.getChildTypes();
                int len = childNames.size();
                children = new ArrayList<>(len);
                for (int i = 0; i < len; i++) {
                    children.add(toArrowField(childTypes.get(i), childNames.get(i), nullable));
                }
                break;
            }
            case MAP: {
                metadata = ImmutableMap.of(ORIGINAL_TYPE, MAP_TYPE);
                arrowType = new ArrowType.Map(false);
                List<ColumnType> childTypes = columnType.getChildTypes();
                if (childTypes.size() != 2)
                    throw new RuntimeException(
                            "Map column expects two child but got " + childTypes.size());

                Field keyField = toArrowField(childTypes.get(0), MapVector.KEY_NAME, false);
                Field valueField = toArrowField(childTypes.get(1), MapVector.VALUE_NAME, true);
                List<Field> kv = Arrays.asList(keyField, valueField);
                Field structField = new Field(MapVector.DATA_VECTOR_NAME,
                        new FieldType(false, ArrowType.Struct.INSTANCE, null), kv);
                children = Collections.singletonList(structField);
                break;
            }
            case STRUCT: {
                arrowType = ArrowType.Struct.INSTANCE;
                List<Integer> fieldIndex = columnType.getFieldIndex();
                List<String> names = columnType.getChildNames();
                List<ColumnType> types = columnType.getChildTypes();
                children = new ArrayList<>(fieldIndex.size());
                for (Integer i : fieldIndex) {
                    children.add(toArrowField(types.get(i), names.get(i), true));
                }
                break;
            }
            default:
                throw new IllegalArgumentException("Unknown Support Type");
        }
        return new Field(columnName, new FieldType(nullable, arrowType, null, metadata), children);
    }
}
