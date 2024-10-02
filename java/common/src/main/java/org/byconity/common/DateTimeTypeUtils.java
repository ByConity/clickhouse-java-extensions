package org.byconity.common;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DateTimeTypeUtils {
    public static Map<ColumnType.TypeValue, TimeUnit> TIMESTAMP_UNIT_MAPPING = new HashMap<>();

    static {
        TIMESTAMP_UNIT_MAPPING.put(ColumnType.TypeValue.DATETIME_MICROS, TimeUnit.MICROSECONDS);
        TIMESTAMP_UNIT_MAPPING.put(ColumnType.TypeValue.DATETIME_MILLIS, TimeUnit.MILLISECONDS);
        // https://spark.apache.org/docs/3.1.3/api/java/org/apache/spark/sql/types/TimestampType.html
        TIMESTAMP_UNIT_MAPPING.put(ColumnType.TypeValue.DATETIME, TimeUnit.MICROSECONDS);
    }

    private static final long MILLI = 1000;
    private static final long MICRO = 1_000_000;
    private static final long NANO = 1_000_000_000;

    public static Instant getTimestamp(long value, TimeUnit timeUnit) {
        long seconds = 0L;
        long nanoseconds = 0L;

        switch (timeUnit) {
            case SECONDS:
                seconds = value;
                nanoseconds = 0;
                break;

            case MILLISECONDS:
                seconds = value / MILLI;
                nanoseconds = (value % MILLI) * MICRO;
                break;

            case MICROSECONDS:
                seconds = value / MICRO;
                nanoseconds = (value % MICRO) * MILLI;
                break;

            case NANOSECONDS:
                seconds = value / NANO;
                nanoseconds = (value % NANO);
                break;
            default:
                throw new IllegalArgumentException("Unknown support time unit " + timeUnit);
        }
        return Instant.ofEpochSecond(seconds, nanoseconds);
    }

    public static long instantToValue(Instant instant, TimeUnit timeUnit) {
        long seconds = instant.getEpochSecond();
        long nanoseconds = instant.getNano();

        switch (timeUnit) {
            case SECONDS:
                return seconds;
            case MILLISECONDS:
                return (seconds * MILLI) + (nanoseconds / MICRO);
            case MICROSECONDS:
                return (seconds * MICRO) + (nanoseconds / MILLI);
            case NANOSECONDS:
                return (seconds * NANO) + nanoseconds;
            default:
                throw new IllegalArgumentException("Unknown support time unit " + timeUnit);
        }
    }
}
