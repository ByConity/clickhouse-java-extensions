package org.byconity.iceberg.cli;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;

import java.util.List;

public class ShowDatabaseProcessor extends ActionProcessor {

    public static void process(Catalog catalog) throws Exception {
        new ShowDatabaseProcessor(catalog).doProcess();
    }

    private ShowDatabaseProcessor(Catalog catalog) {
        super(catalog);
    }

    @Override
    protected void doProcess() throws Exception {
        List<Namespace> namespaces = ((SupportsNamespaces) catalog).listNamespaces();
        System.out.print("Databases in catalog:\n");
        for (Namespace namespace : namespaces) {
            System.out.printf("\t%s\n", namespace);
        }
    }
}
