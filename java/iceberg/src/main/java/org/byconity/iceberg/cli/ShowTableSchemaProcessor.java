package org.byconity.iceberg.cli;

import com.google.common.base.Preconditions;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

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
        TableIdentifier identifier = TableIdentifier.of(database, table);
        if (!catalog.tableExists(identifier)) {
            System.out.printf("Table '%s.%s' does not exist.\n", database, table);
            return;
        }
        Table icebergTable = catalog.loadTable(identifier);
        System.out.printf("Table '%s.%s' schema:\n", database, table);
        System.out.println("\tFields:");
        for (Types.NestedField field : icebergTable.schema().columns()) {
            System.out.printf("\t\t%s\n", field.toString());
        }
        System.out.println("\tPartitionFields:");
        for (PartitionField partitionField : icebergTable.spec().fields()) {
            System.out.printf("\t\t%s\n", partitionField);
        }
        System.out.println("\tSchemaProperties:");
        for (Map.Entry<String, String> kv : icebergTable.properties().entrySet()) {
            System.out.printf("\t\t%s:%s\n", kv.getKey(), kv.getValue());
        }
    }
}
