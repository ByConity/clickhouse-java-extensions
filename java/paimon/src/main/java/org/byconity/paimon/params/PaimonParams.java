package org.byconity.paimon.params;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PaimonParams {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaimonParams.class);
    protected static final Gson GSON = new GsonBuilder().create();
    private static final String METASTORE_TYPE = "metastoreType";
    private JsonObject originParams;

    public static PaimonParams parseFrom(String content) {
        JsonObject json = JsonParser.parseString(content).getAsJsonObject();
        Preconditions.checkState(json.has(METASTORE_TYPE), "missing field " + METASTORE_TYPE);
        MetastoreType metastoreType = MetastoreType.valueOf(json.get(METASTORE_TYPE).getAsString());
        final PaimonParams params;
        switch (metastoreType) {
            case hive:
                params = PaimonHiveMetastoreParams.parseFrom(json);
                break;
            case filesystem:
                params = PaimonFilesystemParams.parseFrom(json);
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

    protected abstract void fillOptions(Options options);

    public final Catalog getCatalog() {
        Options options = new Options();
        fillOptions(options);
        CatalogContext context = CatalogContext.create(options);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("create catalog options: {}", options.toMap());
        }
        return CatalogFactory.createCatalog(context);
    }

    public enum MetastoreType {
        hive, filesystem
    }
}
