package org.byconity.paimon;

import org.apache.paimon.data.Timestamp;
import org.byconity.paimon.util.TimeUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class PaimonTimestampTest {

    @Test
    public void test() {
        LocalDateTime localDateTime =
                LocalDateTime.parse("2024-09-17 10:10:10", TimeUtils.DATETIME_PATTERNS.get(0));
        Instant instant = localDateTime.toInstant(ZoneOffset.UTC);

        Assert.assertEquals("2024-09-17T10:10:10", localDateTime.toString());
        Assert.assertEquals("2024-09-17T10:10:10Z", instant.toString());

        Timestamp timestamp1 = Timestamp.fromLocalDateTime(localDateTime);
        Timestamp timestamp2 = Timestamp.fromInstant(instant);
        Assert.assertEquals(timestamp1, timestamp2);
    }
}
