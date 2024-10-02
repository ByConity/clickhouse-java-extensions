package org.byconity.common;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

public class ArrowColumnVector extends WritableColumnVector {

    private FieldVector arrowVector;

    public ArrowColumnVector(ColumnType type, int capacity, FieldVector arrowVector) {
        super(type, capacity);
        this.arrowVector = arrowVector;
        allocateNew(capacity);
    }

    private ArrowColumnVector(ColumnType type, FieldVector arrowVector) {
        super(type, arrowVector.getValueCapacity());
        this.arrowVector = arrowVector;
    }

    FieldVector getArrowVector() {
        return arrowVector;
    }

    protected void allocateNew(int capacity) {
        int typeSize = type.getPrimitiveTypeValueSize();
        if (type.isUnknown()) {
            // do nothing
        } else {
            arrowVector.setInitialCapacity(capacity);
            arrowVector.allocateNew();
        }
    }

    @Override
    public void reset() {
        arrowVector.reset();
    }

    @Override
    protected void reserveInternal(int capacity) {
        arrowVector.reAlloc();
    }

    @Override
    protected void releaseMemory() {
        arrowVector.clear();
    }

    @Override
    public void putNull(int rowId) {
        arrowVector.setNull(rowId);
    }

    @Override
    public void putBoolean(int rowId, boolean value) {
        ((BitVector) arrowVector).setSafe(rowId, value ? 1 : 0);
    }

    @Override
    public void putByte(int rowId, byte value) {
        ((TinyIntVector) arrowVector).setSafe(rowId, value);
    }

    @Override
    public void putShort(int rowId, short value) {
        ((SmallIntVector) arrowVector).setSafe(rowId, value);
    }

    @Override
    public void putInt(int rowId, int value) {
        ((IntVector) arrowVector).setSafe(rowId, value);
    }

    @Override
    public void putLong(int rowId, long value) {
        ((BigIntVector) arrowVector).setSafe(rowId, value);
    }

    @Override
    public void putFloat(int rowId, float value) {
        ((Float4Vector) arrowVector).setSafe(rowId, value);
    }

    @Override
    public void putDouble(int rowId, double value) {
        ((Float8Vector) arrowVector).setSafe(rowId, value);
    }

    @Override
    public void putByteArray(int rowId, byte[] value, int offset, int count) {
        ((VarBinaryVector) arrowVector).setSafe(rowId, value, offset, count);
    }

    @Override
    public void putString(int rowId, String str) {
        ((VarCharVector) arrowVector).setSafe(rowId, str.getBytes());
    }

    @Override
    public void putDate(int rowId, LocalDate date) {
        ((DateDayVector) arrowVector).setSafe(rowId, (int) date.toEpochDay());
    }

    @Override
    public void putTimestamp(int rowId, LocalDateTime value) {
        TimeStampVector tsVector = (TimeStampVector) arrowVector;
        Instant instant = value.toInstant(ZoneOffset.UTC);
        tsVector.setSafe(rowId, DateTimeTypeUtils.instantToValue(instant, type.getTimeUnit()));
    }

    @Override
    public void putDecimal(int rowId, BigDecimal decimal) {
        decimal = decimal.setScale(type.scale, RoundingMode.UNNECESSARY);
        ((DecimalVector) arrowVector).setSafe(rowId, decimal);
    }

    @Override
    public void putArray(int rowId, List<ColumnValue> v) {
        ListVector listVector = (ListVector) arrowVector;
        int offset = listVector.startNewValue(rowId);
        FieldVector dataVector = listVector.getDataVector();
        ArrowColumnVector childVector =
                new ArrowColumnVector(type.getChildTypes().get(0), dataVector);
        for (int i = 0; i < v.size(); i++) {
            childVector.putValue(offset + i, v.get(i));
        }
        listVector.endValue(rowId, v.size());
    }

    @Override
    public void putMap(int rowId, List<ColumnValue> keys, List<ColumnValue> values) {
        MapVector mapVector = (MapVector) arrowVector;
        int offset = mapVector.startNewValue(rowId);

        StructVector entries = (StructVector) mapVector.getDataVector();
        ArrowColumnVector keyVector = new ArrowColumnVector(type.getChildTypes().get(0),
                entries.getChild(MapVector.KEY_NAME));
        ArrowColumnVector valueVector = new ArrowColumnVector(type.getChildTypes().get(1),
                entries.getChild(MapVector.VALUE_NAME));

        for (int i = 0; i < values.size(); i++) {
            keyVector.putValue(offset + i, keys.get(i));
            valueVector.putValue(offset + i, values.get(i));
            entries.setIndexDefined(offset + i);
        }
        mapVector.endValue(rowId, values.size());
    }

    @Override
    public void putStruct(int rowId, List<ColumnValue> values) {
        StructVector structVector = (StructVector) arrowVector;
        List<String> childNames = type.getChildNames();
        List<ColumnType> childTypes = type.getChildTypes();

        structVector.setIndexDefined(rowId);
        for (int i = 0; i < childNames.size(); i++) {
            ArrowColumnVector childVector = new ArrowColumnVector(childTypes.get(i),
                    structVector.getChild(childNames.get(i)));
            childVector.putValue(rowId, values.get(i));
        }
    }
}
