package org.byconity.common;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ObjectColumnValue implements ColumnValue {

    private Object value;
    private List<ColumnValue> children;

    private List<ColumnValue> mapKeys;
    private List<ColumnValue> mapValues;

    public ObjectColumnValue(Object v) {
        this.value = v;
    }

    /// for array/struct test
    public ObjectColumnValue(List<ColumnValue> children) {
        this.value = 0; /// hack to make not null
        this.children = children;
    }

    /// for map test
    public ObjectColumnValue(List<ColumnValue> keys, List<ColumnValue> values) {
        this.value = 0;
        this.mapKeys = keys;
        this.mapValues = values;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public boolean getBoolean() {
        return (boolean) value;
    }

    @Override
    public short getShort() {
        return (short) value;
    }

    @Override
    public int getInt() {
        return (int) value;
    }

    @Override
    public float getFloat() {
        return (float) value;
    }

    @Override
    public long getLong() {
        return (long) value;
    }

    @Override
    public double getDouble() {
        return (double) value;
    }

    @Override
    public String getString() {
        return (String) value;
    }

    @Override
    public LocalDate getDate() {
        return LocalDate.ofEpochDay((long) value);
    }

    @Override
    public LocalDateTime getDateTime(TimeUnit unit) {
        switch (unit) {
            case NANOSECONDS:
                long epochSecond = ((long) value) / 1000000000;
                long nanoAdjustment = ((long) value) % 1000000000;
                return LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSecond, nanoAdjustment),
                        ZONE_UTC);
            case MILLISECONDS:
                return LocalDateTime.ofInstant(Instant.ofEpochMilli(((long) value)), ZONE_UTC);
            default:
                return LocalDateTime.ofInstant(Instant.ofEpochSecond(((long) value)), ZONE_UTC);
        }
    }

    @Override
    public byte[] getBytes() {
        return (byte[]) value;
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        for (ColumnValue c : children) {
            values.add(c);
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> k, List<ColumnValue> v) {
        int n = this.mapKeys.size();
        for (int i = 0; i < n; i++) {
            k.add(this.mapKeys.get(i));
            v.add(this.mapValues.get(i));
        }
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        for (Integer i : structFieldIndex) {
            values.add(children.get(i));
        }
    }

    @Override
    public byte getByte() {
        return 0;
    }

    @Override
    public BigDecimal getDecimal() {
        return (BigDecimal) value;
    }
}
