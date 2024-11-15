package org.byconity.paimon.cli;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;

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
        if (!catalog.databaseExists(database)) {
            System.out.printf("Database '%s' does not exist.\n", database);
            return;
        }

        List<String> tables = catalog.listTables(database);
        for (String table : tables) {
            catalog.dropTable(Identifier.create(database, table), false);
            System.out.printf("Dropping table '%s.%s' ...\n", database, table);
        }

        catalog.dropDatabase(database, false, false);
        System.out.printf("Drop database '%s' successfully.\n", database);
    }
}
