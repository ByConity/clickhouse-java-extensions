package org.byconity.common;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.TimeUnit;

public interface ColumnValue {

    ZoneId ZONE_UTC = ZoneId.of("UTC");

    boolean isNull();

    boolean getBoolean();

    short getShort();

    int getInt();

    float getFloat();

    long getLong();

    double getDouble();

    String getString();

    LocalDate getDate();

    /**
     * Return the LocalDateTime of time zone UTC
     */
    LocalDateTime getDateTime(TimeUnit unit);

    byte[] getBytes();

    void unpackArray(List<ColumnValue> values);

    void unpackMap(List<ColumnValue> keys, List<ColumnValue> values);

    void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values);

    byte getByte();

    BigDecimal getDecimal();
}
