package org.byconity.iceberg.params;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.hadoop.conf.Configuration;
import org.byconity.common.util.S3Utils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class IcebergS3Params extends IcebergHadoopParams {
    // fs.s3a.endpoint.region is supported after 3.2.2
    // https://issues.apache.org/jira/browse/HADOOP-17705
    private static final Set<String> S3_REQUIRED_OPTIONS = Sets.newHashSet("fs.s3a.endpoint",
            "fs.s3a.access.key", "fs.s3a.secret.key", "fs.s3a.path.style.access");
    private final Map<String, String> s3Options = Maps.newHashMap();

    public static IcebergS3Params parseFrom(JsonObject json) {
        List<String> s3Options = json.keySet().stream().filter((key) -> key.startsWith("fs.s3"))
                .collect(Collectors.toList());
        S3_REQUIRED_OPTIONS.forEach(
                s -> Preconditions.checkState(s3Options.contains(s), "missing field " + s));
        IcebergS3Params params = GSON.fromJson(json, IcebergS3Params.class);
        for (String option : s3Options) {
            JsonElement jsonElement = json.get(option);
            if (!jsonElement.isJsonPrimitive()) {
                continue;
            }
            JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
            if (jsonPrimitive.isString()) {
                params.s3Options.put(option, jsonPrimitive.getAsString());
            } else {
                params.s3Options.put(option, jsonPrimitive.toString());
            }
        }
        return params;
    }

    @Override
    public final FilesystemType getFilesystemType() {
        return FilesystemType.S3;
    }

    @Override
    protected final void fillOptions(Configuration hadoopConf) {
        super.fillOptions(hadoopConf);

        for (String supportedProtocol : S3Utils.SUPPORTED_PROTOCOLS) {
            hadoopConf.set(String.format("fs.%s.impl", supportedProtocol.toLowerCase()),
                    "org.apache.hadoop.fs.s3a.S3AFileSystem");
        }
        s3Options.forEach(hadoopConf::set);
    }
}
