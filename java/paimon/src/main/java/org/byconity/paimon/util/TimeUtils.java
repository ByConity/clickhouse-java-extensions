package org.byconity.paimon.util;

import com.google.common.collect.Lists;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public abstract class TimeUtils {
    public static final ZoneId ZONE_UTC = ZoneId.of("UTC");
    public static final ZoneOffset DEFAULT_ZONE_OFFSET =
            ZonedDateTime.now(ZoneId.systemDefault()).getOffset();
    public static final List<DateTimeFormatter> DATE_PATTERNS = Lists.newArrayList(
            DateTimeFormatter.ofPattern("yyyy-MM-dd"), DateTimeFormatter.ofPattern("yyyyMMdd"));
    public static final List<DateTimeFormatter> DATETIME_PATTERNS =
            Lists.newArrayList(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S"),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SS"),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSS"),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS"),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSS"),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSS"),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS"));
}
