package org.byconity.iceberg.params;

import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;

import java.util.HashMap;

public class IcebergHiveParams extends IcebergParams {

    @SerializedName("uri")
    private String uri;

    @SerializedName("warehouse")
    private String warehouse;

    public static IcebergHiveParams parseFrom(JsonObject json) {
        return GSON.fromJson(json, IcebergHiveParams.class);
    }

    @Override
    public final MetastoreType getMetastoreType() {
        return MetastoreType.hive;
    }

    @Override
    public Catalog getCatalog() {
        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(new Configuration());
        HashMap<String, String> properties = Maps.newHashMap();
        properties.put(CatalogProperties.URI, uri);
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);

        // TODO how to setup s3 properties

        hiveCatalog.initialize("bytehouse-hive-metastore", properties);
        return hiveCatalog;
    }

}
