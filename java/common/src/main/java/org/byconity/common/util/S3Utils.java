package org.byconity.common.util;

import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.Set;

public abstract class S3Utils {
    public static final Set<String> SUPPORTED_PROTOCOLS = Collections.unmodifiableSet(
            Sets.newHashSet("HDFS", "S3", "S3A", "COSN", "COS", "OBS", "OSS", "TOS"));
}
