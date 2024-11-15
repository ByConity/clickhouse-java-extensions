package org.byconity.paimon.params;

import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

public final class PaimonHDFSParams extends PaimonFilesystemParams {

    @SerializedName("warehouse")
    private String warehouse;

    public static PaimonHDFSParams parseFrom(JsonObject json) {
        return GSON.fromJson(json, PaimonHDFSParams.class);
    }

    @Override
    public FilesystemType getFilesystemType() {
        return FilesystemType.HDFS;
    }

    @Override
    protected void fillOptions(Options options) {
        options.set(CatalogOptions.METASTORE, getMetastoreType().name());
        options.set(CatalogOptions.WAREHOUSE, warehouse);
    }
}
