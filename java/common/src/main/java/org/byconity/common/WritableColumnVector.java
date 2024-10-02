package org.byconity.common;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public abstract class WritableColumnVector extends ColumnVector {
    private static final int MAX_CAPACITY = Integer.MAX_VALUE - 15;

    protected int numNulls;
    protected int capacity;
    protected int elementsAppended;

    private WritableColumnVector[] childColumns;

    protected WritableColumnVector(ColumnType type, int capacity) {
        super(type);
        this.numNulls = 0;
        this.elementsAppended = 0;
        this.capacity = capacity;
        /// do reserve on allocate new
    }

    private void throwUnsupportedException(int requiredCapacity, Throwable cause) {
        String message = "Cannot reserve additional contiguous bytes in the vectorized vector ("
                + (requiredCapacity >= 0 ? "requested " + requiredCapacity + " bytes"
                        : "integer overflow).");
        throw new RuntimeException(message, cause);
    }

    private void reserve(int requiredCapacity) {
        if (requiredCapacity < 0) {
            throwUnsupportedException(requiredCapacity, null);
        } else if (requiredCapacity > capacity) {
            int newCapacity = (int) Math.min(MAX_CAPACITY, requiredCapacity * 2L);
            if (requiredCapacity <= newCapacity) {
                try {
                    reserveInternal(newCapacity);
                } catch (OutOfMemoryError outOfMemoryError) {
                    throwUnsupportedException(requiredCapacity, outOfMemoryError);
                }
            } else {
                throwUnsupportedException(requiredCapacity, null);
            }
        }
    }

    public void reset() {
        if (childColumns != null) {
            for (WritableColumnVector c : childColumns) {
                c.reset();
            }
        }
        elementsAppended = 0;
        numNulls = 0;
    }

    @Override
    public void close() {
        if (childColumns != null) {
            for (int i = 0; i < childColumns.length; i++) {
                childColumns[i].close();
                childColumns[i] = null;
            }
            childColumns = null;
        }
        releaseMemory();
    }

    protected abstract void reserveInternal(int capacity);

    protected abstract void releaseMemory();

    public abstract void putNull(int rowId);

    public abstract void putBoolean(int rowId, boolean value);

    public abstract void putByte(int rowId, byte value);

    public abstract void putShort(int rowId, short value);

    public abstract void putInt(int rowId, int value);

    public abstract void putLong(int rowId, long value);

    public abstract void putFloat(int rowId, float value);

    public abstract void putDouble(int rowId, double value);

    public abstract void putByteArray(int rowId, byte[] value, int offset, int count);

    public final void putByteArray(int rowId, byte[] value) {
        putByteArray(rowId, value, 0, value.length);
    }

    public void putString(int rowId, String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        putByteArray(rowId, bytes, 0, bytes.length);
    }

    public abstract void putDate(int rowId, LocalDate date);

    public abstract void putTimestamp(int rowId, LocalDateTime datetime);

    public abstract void putDecimal(int rowId, BigDecimal decimal);

    public abstract void putArray(int rowId, List<ColumnValue> v);

    public abstract void putMap(int rowId, List<ColumnValue> keys, List<ColumnValue> values);

    public abstract void putStruct(int rowId, List<ColumnValue> values);

    public void putValue(int rowId, ColumnValue v) {
        ColumnType.TypeValue typeValue = type.getTypeValue();
        if (v == null || v.isNull()) {
            putNull(rowId);
            return;
        }

        switch (typeValue) {
            case TINYINT:
                putByte(rowId, v.getByte());
                break;
            case BOOLEAN:
                putBoolean(rowId, v.getBoolean());
                break;
            case SHORT:
                putShort(rowId, v.getShort());
                break;
            case INT:
                putInt(rowId, v.getInt());
                break;
            case LONG:
                putLong(rowId, v.getLong());
                break;
            case FLOAT:
                putFloat(rowId, v.getFloat());
                break;
            case DOUBLE:
                putDouble(rowId, v.getDouble());
                break;
            case BINARY:
                putByteArray(rowId, v.getBytes());
                break;
            case STRING:
                putString(rowId, v.getString());
                break;
            case DATE:
                putDate(rowId, v.getDate());
                break;
            case DATETIME:
            case DATETIME_MILLIS:
            case DATETIME_MICROS:
                putTimestamp(rowId, v.getDateTime(type.getTimeUnit()));
                break;
            case DECIMAL64:
            case DECIMAL128:
                putDecimal(rowId, v.getDecimal());
                break;
            case ARRAY: {
                List<ColumnValue> values = new ArrayList<>();
                v.unpackArray(values);
                putArray(rowId, values);
                break;
            }
            case MAP: {
                List<ColumnValue> keys = new ArrayList<>();
                List<ColumnValue> values = new ArrayList<>();
                v.unpackMap(keys, values);
                putMap(rowId, keys, values);
                break;
            }
            case STRUCT: {
                List<ColumnValue> values = new ArrayList<>();
                v.unpackStruct(type.getFieldIndex(), values);
                putStruct(rowId, values);
                break;
            }
            default:
                throw new RuntimeException("Unknown type value: " + typeValue);
        }
    }
}
