package org.byconity.paimon.params;

import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

public class PaimonHiveMetastoreParams extends PaimonParams {

    @SerializedName("uri")
    private String uri;

    @SerializedName("warehouse")
    private String warehouse;

    public static PaimonHiveMetastoreParams parseFrom(JsonObject json) {
        return GSON.fromJson(json, PaimonHiveMetastoreParams.class);
    }

    @Override
    public MetastoreType getMetastoreType() {
        return MetastoreType.hive;
    }

    @Override
    protected void fillOptions(Options options) {
        options.set(CatalogOptions.METASTORE, getMetastoreType().name());
        options.set(CatalogOptions.URI, uri);
        options.set(CatalogOptions.WAREHOUSE, warehouse);
    }
}
