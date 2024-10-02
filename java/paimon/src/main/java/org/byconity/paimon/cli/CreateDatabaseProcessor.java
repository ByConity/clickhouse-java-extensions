package org.byconity.paimon.cli;

import org.apache.paimon.catalog.Catalog;

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
        if (catalog.databaseExists(database)) {
            System.out.printf("Database '%s' already exists.\n", database);
            return;
        }
        catalog.createDatabase(database, true);
        System.out.printf("Create database '%s' successfully.\n", database);
    }
}
