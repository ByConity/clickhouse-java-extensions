package org.byconity.iceberg.params;

import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;

public class IcebergLocalParams extends IcebergHadoopParams {

    public static IcebergLocalParams parseFrom(JsonObject json) {
        return GSON.fromJson(json, IcebergLocalParams.class);
    }

    @Override
    public final FilesystemType getFilesystemType() {
        return FilesystemType.LOCAL;
    }

    @Override
    protected final void fillOptions(Configuration hadoopConf) {
        super.fillOptions(hadoopConf);
    }
}
