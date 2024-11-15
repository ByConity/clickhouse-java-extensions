package org.byconity.iceberg.params;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IcebergParams {

    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergParams.class);
    protected static final Gson GSON = new GsonBuilder().create();
    private static final String METASTORE_TYPE = "metastoreType";
    private JsonObject originParams;

    public static IcebergParams parseFrom(String content) {
        JsonObject json = JsonParser.parseString(content).getAsJsonObject();
        Preconditions.checkState(json.has(METASTORE_TYPE), "missing field " + METASTORE_TYPE);
        MetastoreType metastoreType = MetastoreType.valueOf(json.get(METASTORE_TYPE).getAsString());
        final IcebergParams params;
        switch (metastoreType) {
            case hive:
                params = IcebergHiveParams.parseFrom(json);
                break;
            case hadoop:
                params = IcebergHadoopParams.parseFrom(json);
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unknown metastoreType: %s", metastoreType.name()));
        }
        params.originParams = json;
        return params;
    }

    public String getStringParam(String name) {
        Preconditions.checkState(originParams.has(name), "missing param '" + name + "'");
        return originParams.get(name).getAsString();
    }

    public String getStringParamOrDefault(String name, String defaultValue) {
        if (!originParams.has(name)) {
            return defaultValue;
        }
        return originParams.get(name).getAsString();
    }

    public abstract MetastoreType getMetastoreType();

    public abstract Catalog getCatalog();

    public enum MetastoreType {
        hadoop, hive,
    }
}
