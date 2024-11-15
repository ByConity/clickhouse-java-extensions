package org.byconity.paimon.cli;

import org.apache.paimon.catalog.Catalog;

import java.util.List;

public class ShowDatabaseProcessor extends ActionProcessor {

    public static void process(Catalog catalog) {
        new ShowDatabaseProcessor(catalog).doProcess();
    }

    private ShowDatabaseProcessor(Catalog catalog) {
        super(catalog);
    }

    @Override
    public void doProcess() {
        List<String> databases = catalog.listDatabases();
        System.out.print("Databases in catalog:\n");
        for (String database : databases) {
            System.out.printf("\t%s\n", database);
        }
    }
}
