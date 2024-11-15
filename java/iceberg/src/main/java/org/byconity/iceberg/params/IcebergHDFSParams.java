package org.byconity.iceberg.params;

import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergHDFSParams extends IcebergHadoopParams {
    private final Map<String, String> hdfsOptions = Maps.newHashMap();

    public static IcebergHDFSParams parseFrom(JsonObject json) {
        List<String> hdfsOptions = json
                .keySet().stream().filter((key) -> key.startsWith("hadoop.")
                        || key.startsWith("fs.") || key.startsWith("dfs."))
                .collect(Collectors.toList());
        IcebergHDFSParams params = GSON.fromJson(json, IcebergHDFSParams.class);
        for (String option : hdfsOptions) {
            JsonElement jsonElement = json.get(option);
            if (!jsonElement.isJsonPrimitive()) {
                continue;
            }
            JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
            if (jsonPrimitive.isString()) {
                params.hdfsOptions.put(option, jsonPrimitive.getAsString());
            } else {
                params.hdfsOptions.put(option, jsonPrimitive.toString());
            }
        }
        return params;
    }

    @Override
    public final FilesystemType getFilesystemType() {
        return FilesystemType.HDFS;
    }

    @Override
    protected final void fillOptions(Configuration hadoopConf) {
        super.fillOptions(hadoopConf);
        hdfsOptions.forEach(hadoopConf::set);
    }
}
