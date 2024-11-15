package org.byconity.iceberg.cli;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;

public class CreateDatabaseProcessor extends ActionProcessor {

    private final String database;

    public static void process(Catalog catalog, String arg) throws Exception {
        new CreateDatabaseProcessor(catalog, arg).doProcess();
    }

    private CreateDatabaseProcessor(Catalog catalog, String database) {
        super(catalog);
        this.database = database;
    }

    @Override
    protected void doProcess() throws Exception {
        Namespace namespace = Namespace.of(database);
        if (((SupportsNamespaces) catalog).namespaceExists(namespace)) {
            System.out.printf("Database '%s' already exists.\n", database);
            return;
        }
        ((SupportsNamespaces) catalog).createNamespace(namespace);
        System.out.printf("Create database '%s' successfully.\n", database);
    }
}
