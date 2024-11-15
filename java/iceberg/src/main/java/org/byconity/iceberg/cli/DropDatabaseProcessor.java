package org.byconity.iceberg.cli;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.List;

public class DropDatabaseProcessor extends ActionProcessor {

    private final String database;

    public static void process(Catalog catalog, String arg) throws Exception {
        new DropDatabaseProcessor(catalog, arg).doProcess();
    }

    private DropDatabaseProcessor(Catalog catalog, String database) {
        super(catalog);
        this.database = database;
    }

    @Override
    protected void doProcess() throws Exception {
        Namespace namespace = Namespace.of(database);
        if (!((SupportsNamespaces) catalog).namespaceExists(namespace)) {
            System.out.printf("Database '%s' does not exist.\n", database);
            return;
        }

        List<TableIdentifier> tableIdentifiers = catalog.listTables(namespace);
        for (TableIdentifier tableIdentifier : tableIdentifiers) {
            catalog.dropTable(tableIdentifier);
            System.out.printf("Dropping table '%s' ...\n", tableIdentifier);
        }

        if (((SupportsNamespaces) catalog).dropNamespace(namespace)) {
            System.out.printf("Drop database '%s' successfully.\n", database);
        } else {
            System.out.printf("Failed to drop database '%s'.\n", database);
        }
    }
}
