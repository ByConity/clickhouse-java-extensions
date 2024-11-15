package org.byconity.iceberg.cli;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.List;

public class ShowTableProcessor extends ActionProcessor {

    private final String database;

    public static void process(Catalog catalog, String arg) throws Exception {
        new ShowTableProcessor(catalog, arg).doProcess();
    }

    private ShowTableProcessor(Catalog catalog, String database) {
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

        System.out.printf("Tables in database '%s':\n", database);
        List<TableIdentifier> tableIdentifiers = catalog.listTables(namespace);
        for (TableIdentifier tableIdentifier : tableIdentifiers) {
            System.out.printf("\t%s\n", tableIdentifier.name());
        }
    }
}
