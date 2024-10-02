package org.byconity.paimon.reader;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.types.*;
import org.apache.paimon.utils.InternalRowUtils;
import org.byconity.common.ColumnValue;
import org.byconity.paimon.util.TimeUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class PaimonColumnValue implements ColumnValue {

    private final Object fieldData;
    private final DataType dataType;

    public PaimonColumnValue(Object fieldData, DataType dataType) {
        this.fieldData = fieldData;
        this.dataType = dataType;
    }

    @Override
    public boolean isNull() {
        return fieldData == null;
    }

    @Override
    public boolean getBoolean() {
        return (boolean) fieldData;
    }

    @Override
    public short getShort() {
        return (short) fieldData;
    }

    @Override
    public int getInt() {
        return (int) fieldData;
    }

    @Override
    public float getFloat() {
        return (float) fieldData;
    }

    @Override
    public long getLong() {
        return (long) fieldData;
    }

    @Override
    public double getDouble() {
        return (double) fieldData;
    }

    @Override
    public String getString() {
        return fieldData.toString();
    }

    @Override
    public byte[] getBytes() {
        return (byte[]) fieldData;
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        InternalArray array = (InternalArray) fieldData;
        toPaimonColumnValue(values, array, ((ArrayType) dataType).getElementType());
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        InternalMap map = (InternalMap) fieldData;
        DataType keyType;
        DataType valueType;
        if (dataType instanceof MapType) {
            keyType = ((MapType) dataType).getKeyType();
            valueType = ((MapType) dataType).getValueType();
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }

        InternalArray keyArray = map.keyArray();
        toPaimonColumnValue(keys, keyArray, keyType);

        InternalArray valueArray = map.valueArray();
        toPaimonColumnValue(values, valueArray, valueType);
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        ColumnarRow array = (ColumnarRow) fieldData;
        List<DataField> fields = ((RowType) dataType).getFields();
        for (int i = 0; i < structFieldIndex.size(); i++) {
            Integer idx = structFieldIndex.get(i);
            PaimonColumnValue cv = null;
            if (idx != null) {
                DataField dataField = fields.get(idx);
                Object o = InternalRowUtils.get(array, idx, dataField.type());
                if (o != null) {
                    cv = new PaimonColumnValue(o, dataField.type());
                }
            }
            values.add(cv);
        }
    }

    @Override
    public byte getByte() {
        return (byte) fieldData;
    }

    public BigDecimal getDecimal() {
        return ((Decimal) fieldData).toBigDecimal();
    }

    private void toPaimonColumnValue(List<ColumnValue> values, InternalArray array,
            DataType dataType) {
        for (int i = 0; i < array.size(); i++) {
            PaimonColumnValue cv = null;
            Object o = InternalRowUtils.get(array, i, dataType);
            if (o != null) {
                cv = new PaimonColumnValue(o, dataType);
            }
            values.add(cv);
        }
    }

    @Override
    public LocalDate getDate() {
        return LocalDate.ofEpochDay((int) fieldData);
    }

    @Override
    public LocalDateTime getDateTime(TimeUnit unit) {
        if (Objects.requireNonNull(
                dataType.getTypeRoot()) == DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
            return LocalDateTime.ofInstant(((Timestamp) fieldData).toInstant(), TimeUtils.ZONE_UTC);
        }
        throw new UnsupportedOperationException("Unsupported type: ");
    }
}
