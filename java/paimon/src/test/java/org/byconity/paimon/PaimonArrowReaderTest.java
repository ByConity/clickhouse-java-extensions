package org.byconity.paimon;

import com.google.common.collect.Maps;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.ThreadContext;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.byconity.paimon.reader.PaimonArrowReaderBuilder;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class PaimonArrowReaderTest extends PaimonTestBase {

    private static final String DB_NAME = "test_db";
    private static final String TABLE_NAME = "test_table";

    private static final String COL_INT_NAME = "col_int";
    private static final String COL_DOUBLE_NAME = "col_double";
    private static final String COL_STR_NAME = "col_str";

    @Before
    public void before() {
        ThreadContext.put("QueryId", UUID.randomUUID().toString());
    }

    private Table createTable(Catalog catalog) throws Exception {
        catalog.createDatabase(DB_NAME, true);
        Identifier tableId = Identifier.create(DB_NAME, TABLE_NAME);
        catalog.dropTable(tableId, true);

        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.primaryKey(COL_INT_NAME, COL_STR_NAME);
        schemaBuilder.partitionKeys(COL_INT_NAME);
        schemaBuilder.column(COL_INT_NAME, DataTypes.INT());
        schemaBuilder.column(COL_DOUBLE_NAME, DataTypes.DOUBLE());
        schemaBuilder.column(COL_STR_NAME, DataTypes.STRING());
        Schema schema = schemaBuilder.build();
        catalog.createTable(tableId, schema, false);

        return catalog.getTable(tableId);
    }

    private void writeData(Table table) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder().withOverwrite();

        try (BatchTableWrite write = writeBuilder.newWrite()) {
            GenericRow record1 = GenericRow.of(1, 1.1, BinaryString.fromString("par1"));
            GenericRow record2 = GenericRow.of(2, 2.2, BinaryString.fromString("par2"));
            GenericRow record3 = GenericRow.of(3, 3.3, BinaryString.fromString("par3"));

            write.write(record1, 1);
            write.write(record2, 1);
            write.write(record3, 1);

            List<CommitMessage> messages = write.prepareCommit();

            try (BatchTableCommit commit = writeBuilder.newCommit()) {
                commit.commit(messages);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void readData(PaimonMetaClient client) throws Throwable {
        String requestFields =
                String.format("%s,%s,%s", COL_INT_NAME, COL_DOUBLE_NAME, COL_STR_NAME);

        Map<String, String> metaParams = Maps.newHashMap();
        metaParams.put("database", DB_NAME);
        metaParams.put("table", TABLE_NAME);
        metaParams.put("required_fields", requestFields);
        Map<String, Object> paimonScanInfo = client.getPaimonScanInfo(metaParams);
        String encodedTable = new String((byte[]) paimonScanInfo.get("encoded_table"));
        List<String> encodedSplits = ((List<byte[]>) paimonScanInfo.get("encoded_splits")).stream()
                .map(String::new).collect(Collectors.toList());

        Map<String, String> readerParams = Maps.newHashMap();
        readerParams.put("encoded_table", encodedTable);
        readerParams.put("fetch_size", "4096");
        readerParams.put("required_fields", requestFields);
        readerParams.put("encoded_splits", String.join(",", encodedSplits));

        PaimonArrowReaderBuilder builder = PaimonArrowReaderBuilder
                .create(GSON.toJson(readerParams).getBytes(StandardCharsets.UTF_8));
        try (ArrowReader reader = builder.build()) {
            reader.loadNextBatch();
        }
    }

    @Test
    public void testReader() throws Throwable {
        PaimonMetaClient client = PaimonMetaClient
                .create(GSON.toJson(createLocalParams()).getBytes(StandardCharsets.UTF_8));
        Catalog catalog = client.getCatalog();

        Table table = createTable(catalog);

        writeData(table);

        readData(client);
    }
}
