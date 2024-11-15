package org.byconity.common.util;

public abstract class ExceptionUtils {
    @SuppressWarnings("unchecked")
    public static <T extends Throwable> void rethrow(Throwable t) throws T {
        throw (T) t; // Cast the Throwable to a generic type and throw it
    }
}
