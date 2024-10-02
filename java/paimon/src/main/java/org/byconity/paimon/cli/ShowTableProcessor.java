package org.byconity.paimon.cli;

import org.apache.paimon.catalog.Catalog;

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
        if (!catalog.databaseExists(database)) {
            System.out.printf("Database '%s' does not exist.\n", database);
            return;
        }

        System.out.printf("Tables in database '%s':\n", database);
        List<String> tables = catalog.listTables(database);
        for (String table : tables) {
            System.out.printf("\t%s\n", table);
        }
    }
}
