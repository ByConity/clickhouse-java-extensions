package org.byconity.paimon.params;

import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

public final class PaimonLocalFilesystemParams extends PaimonFilesystemParams {

    @SerializedName("path")
    private String path;

    public static PaimonLocalFilesystemParams parseFrom(JsonObject json) {
        return GSON.fromJson(json, PaimonLocalFilesystemParams.class);
    }

    @Override
    public FilesystemType getFilesystemType() {
        return FilesystemType.LOCAL;
    }

    @Override
    protected void fillOptions(Options options) {
        options.set(CatalogOptions.METASTORE, getMetastoreType().name());
        options.set(CatalogOptions.WAREHOUSE, new Path(path).toUri().toString());
    }
}
