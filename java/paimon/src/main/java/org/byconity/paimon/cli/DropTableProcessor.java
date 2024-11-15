package org.byconity.paimon.cli;

import com.google.common.base.Preconditions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;

public class DropTableProcessor extends ActionProcessor {
    private final String database;
    private final String table;

    public static void process(Catalog catalog, String arg) throws Exception {
        String[] segments = arg.split("\\.");
        Preconditions.checkState(segments.length == 2, String.format(
                "Arg for action '%s' should be <database>.<table>.", ActionType.DROP_TABLE.name()));
        new DropTableProcessor(catalog, segments[0], segments[1]).doProcess();
    }

    private DropTableProcessor(Catalog catalog, String database, String table) {
        super(catalog);
        this.database = database;
        this.table = table;
    }

    @Override
    protected void doProcess() throws Exception {
        Identifier identifier = Identifier.create(database, table);
        if (!catalog.tableExists(identifier)) {
            System.out.printf("Table '%s.%s' does not exist.\n", database, table);
            return;
        }

        catalog.dropTable(identifier, false);
        System.out.printf("Drop table '%s.%s' successfully.\n", database, table);
    }
}
