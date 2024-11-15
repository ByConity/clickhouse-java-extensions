package org.byconity.iceberg.util;

import java.nio.ByteBuffer;
import java.util.UUID;

public abstract class UUIDUtils {
    // Convert UUID to a byte array
    public static byte[] UUIDToBytes(UUID uuid) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return buffer.array();
    }
}
