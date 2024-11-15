package org.byconity.iceberg.cli;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.iceberg.catalog.Catalog;

public abstract class ActionProcessor {
    protected static final Gson GSON = new GsonBuilder().create();

    protected final Catalog catalog;

    protected ActionProcessor(Catalog catalog) {
        this.catalog = catalog;
    }

    protected abstract void doProcess() throws Exception;
}
