package org.byconity.las;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.byconity.common.ColumnValue;
import org.byconity.common.DateTimeTypeUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LasColumnValue implements ColumnValue {
    private Object fieldData;
    private final ObjectInspector fieldInspector;

    public LasColumnValue(ObjectInspector fieldInspector) {
        this.fieldData = null;
        this.fieldInspector = fieldInspector;
    }

    public LasColumnValue(Object fieldData, ObjectInspector fieldInspector) {
        this.fieldData = fieldData;
        this.fieldInspector = fieldInspector;
    }

    public void setData(Object fieldData) {
        this.fieldData = fieldData;
    }

    private Object inspectObject() {
        return ((PrimitiveObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData);
    }

    @Override
    public boolean isNull() {
        return fieldData == null;
    }

    @Override
    public boolean getBoolean() {
        return (boolean) inspectObject();
    }

    @Override
    public short getShort() {
        return (short) inspectObject();
    }

    @Override
    public int getInt() {
        return (int) inspectObject();
    }

    @Override
    public float getFloat() {
        return (float) inspectObject();
    }


    @Override
    public long getLong() {
        return (long) inspectObject();
    }


    @Override
    public double getDouble() {
        return (double) inspectObject();
    }


    @Override
    public String getString() {
        return inspectObject().toString();
    }

    @Override
    public LocalDate getDate() {
        Date day = ((DateObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData);
        return day.toLocalDate();
    }

    @Override
    public LocalDateTime getDateTime(TimeUnit unit) {
        if (fieldData instanceof Timestamp) {
            return LocalDateTime.ofInstant(((Timestamp) fieldData).toInstant(), ZONE_UTC);
        } else if (fieldData instanceof LongWritable) {
            long datetime = ((LongWritable) fieldData).get();
            Instant instant = DateTimeTypeUtils.getTimestamp(datetime, unit);
            return LocalDateTime.ofInstant(instant, ZONE_UTC);
        } else {
            java.sql.Timestamp timestamp =
                    ((TimestampObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData);
            return LocalDateTime.ofInstant(timestamp.toInstant(), ZONE_UTC);
        }
    }

    @Override
    public byte[] getBytes() {
        return (byte[]) inspectObject();
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        ListObjectInspector inspector = (ListObjectInspector) fieldInspector;
        List<?> items = inspector.getList(fieldData);
        ObjectInspector itemInspector = inspector.getListElementObjectInspector();
        for (Object item : items) {
            values.add(new LasColumnValue(item, itemInspector));
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        MapObjectInspector inspector = (MapObjectInspector) fieldInspector;
        ObjectInspector keyObjectInspector = inspector.getMapKeyObjectInspector();
        ObjectInspector valueObjectInspector = inspector.getMapValueObjectInspector();
        for (Map.Entry kv : inspector.getMap(fieldData).entrySet()) {
            LasColumnValue k = new LasColumnValue(kv.getKey(), keyObjectInspector);
            LasColumnValue v = new LasColumnValue(kv.getValue(), valueObjectInspector);
            keys.add(k);
            values.add(v);
        }
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        StructObjectInspector inspector = (StructObjectInspector) fieldInspector;
        List<? extends StructField> fields = inspector.getAllStructFieldRefs();
        for (int i = 0; i < structFieldIndex.size(); i++) {
            Integer idx = structFieldIndex.get(i);
            ColumnValue cv = null;
            if (idx != null) {
                StructField sf = fields.get(idx);
                Object o = inspector.getStructFieldData(fieldData, sf);
                cv = new LasColumnValue(o, sf.getFieldObjectInspector());
            }
            values.add(cv);
        }
    }

    @Override
    public byte getByte() {
        return 0;
    }

    @Override
    public BigDecimal getDecimal() {
        return ((HiveDecimal) inspectObject()).bigDecimalValue();
    }
}
