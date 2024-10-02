package org.byconity.paimon.params;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.annotations.SerializedName;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PaimonS3Params extends PaimonFilesystemParams {
    private static final Set<String> S3_REQUIRED_OPTIONS =
            Sets.newHashSet("s3.endpoint.region", "s3.endpoint", "s3.access-key", "s3.secret-key");

    @SerializedName("warehouse")
    private String warehouse;

    private final Map<String, String> s3Options = Maps.newHashMap();

    public static PaimonS3Params parseFrom(JsonObject json) {
        List<String> s3Options = json.keySet().stream()
                .filter((key) -> key.startsWith("s3.") || key.startsWith("fs.s3"))
                .collect(Collectors.toList());
        S3_REQUIRED_OPTIONS.forEach(
                s -> Preconditions.checkState(s3Options.contains(s), "missing field " + s));
        PaimonS3Params params = GSON.fromJson(json, PaimonS3Params.class);
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
    public FilesystemType getFilesystemType() {
        return FilesystemType.S3;
    }

    @Override
    protected void fillOptions(Options options) {
        options.set(CatalogOptions.METASTORE, getMetastoreType().name());
        s3Options.forEach(options::set);
        options.set(CatalogOptions.WAREHOUSE, warehouse);
    }
}
