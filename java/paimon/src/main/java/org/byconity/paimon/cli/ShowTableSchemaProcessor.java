package org.byconity.paimon.cli;

import com.google.common.base.Preconditions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;

import java.util.Map;

public class ShowTableSchemaProcessor extends ActionProcessor {
    private final String database;
    private final String table;

    public static void process(Catalog catalog, String arg) throws Exception {
        String[] segments = arg.split("\\.");
        Preconditions.checkState(segments.length == 2,
                String.format("Arg for action '%s' should be <database>.<table>.",
                        ActionType.SHOW_TABLE_SCHEMA.name()));
        new ShowTableSchemaProcessor(catalog, segments[0], segments[1]).doProcess();
    }

    private ShowTableSchemaProcessor(Catalog catalog, String database, String table) {
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
        Table paimonTable = catalog.getTable(identifier);
        System.out.printf("Table '%s.%s' schema:\n", database, table);
        System.out.println("\tFields:");
        for (DataField field : paimonTable.rowType().getFields()) {
            System.out.printf("\t\t%s\n", field.toString());
        }
        System.out.println("\tPrimaryKeys:");
        for (String primaryKey : paimonTable.primaryKeys()) {
            System.out.printf("\t\t%s\n", primaryKey);
        }
        System.out.println("\tPartitionKeys:");
        for (String partitionKey : paimonTable.partitionKeys()) {
            System.out.printf("\t\t%s\n", partitionKey);
        }
        System.out.println("\tSchemaOptions:");
        for (Map.Entry<String, String> kv : paimonTable.options().entrySet()) {
            System.out.printf("\t\t%s:%s\n", kv.getKey(), kv.getValue());
        }
    }
}
