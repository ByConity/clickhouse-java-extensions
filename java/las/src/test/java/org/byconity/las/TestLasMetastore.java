package org.byconity.las;

import com.volcengine.las.fs.sdk.shaded.cn.hutool.poi.excel.ExcelPicUtil;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.byconity.las.reader.LasInputSplitArrowReaderBuilder;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestLasMetastore {

    Map<String, String> getParams() {
        Map<String, String> params = new HashMap<>();
        params.put("access_key", System.getenv("LAS_ACCESS_KEY"));
        params.put("secret_key", System.getenv("LAS_SECRET_KEY"));
        params.put("region", "cn-beijing");
        params.put("endpoint", System.getenv("LAS_FS_ENDPOINT"));
        return params;
    }


    void testScan(Map<String, String> params) throws Exception {
        long getTableElapsed = 0;
        long listPartitionsElapsed = 0;
        long prepareSplitElapsed = 0;
        long scanElapsed = 0;

        LasMetaClient client = new LasMetaClient(params);

        getTableElapsed = System.currentTimeMillis();
        Table hiveTable = client.metaStoreClient.getTable(client.databaseName, client.tableName);
        getTableElapsed = System.currentTimeMillis() - getTableElapsed;
        System.out.println(hiveTable);
        listPartitionsElapsed = System.currentTimeMillis();
        List<Partition> partition = client.metaStoreClient.listPartitions(client.databaseName,
                client.tableName, (short) 1000);
        List<String> requiredPartitions =
                partition.stream().map(p -> p.getSd().getLocation()).collect(Collectors.toList());
        System.out.println("required partitions " + requiredPartitions.size());
        listPartitionsElapsed = System.currentTimeMillis() - listPartitionsElapsed;

        prepareSplitElapsed = System.currentTimeMillis();
        List<OptimizedHadoopInputSplit> inputSplits =
                client.createSplits(1, 16, requiredPartitions);
        System.out.println("Get " + inputSplits.size() + " inputSplits");
        prepareSplitElapsed = System.currentTimeMillis() - prepareSplitElapsed;

        String columnNames = client.getColumnNames();
        String columnTypes = client.getColumnTypes();
        // List<FieldSchema> fields = hiveTable.getSd().getCols();
        // String columnNames =
        // fields.stream().map(FieldSchema::getName).collect(Collectors.joining(","));
        // String columnTypes =
        // fields.stream().map(FieldSchema::getType).collect(Collectors.joining("#"));
        params.put("fetch_size", "8192");
        params.put("hive_column_names", columnNames);
        params.put("hive_column_types", columnTypes);
        params.put("required_fields", params.getOrDefault("required_columns", columnNames));
        params.put("input_format", hiveTable.getSd().getInputFormat());
        params.put("serde", hiveTable.getSd().getSerdeInfo().getSerializationLib());

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : hiveTable.getParameters().entrySet()) {
            if (sb.length() > 0)
                sb.append('#');
            sb.append(entry.getKey());
            sb.append('=');
            sb.append(entry.getValue());
        }
        params.put("fs_options_props", sb.toString());

        scanElapsed = System.currentTimeMillis();
        for (OptimizedHadoopInputSplit inputSplit : inputSplits) {
            /// serialize
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(inputSplit);
            oos.flush();
            byte[] serializedBytes = baos.toByteArray();

            /// deserialize
            ByteArrayInputStream bais = new ByteArrayInputStream(serializedBytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            OptimizedHadoopInputSplit split = (OptimizedHadoopInputSplit) ois.readObject();

            /// read
            LasInputSplitArrowReaderBuilder builder =
                    new LasInputSplitArrowReaderBuilder(split, new RootAllocator(), params);
            System.out.println(builder);
            try (ArrowReader stream = builder.build()) {
                while (stream.loadNextBatch()) {
                    System.out.println(stream.getVectorSchemaRoot().contentToTSVString());
                }
            }
        }
        scanElapsed = System.currentTimeMillis() - scanElapsed;

        System.out.printf(
                "Summary: getTable elapsed %d ms, listPartitions elapsed %d ms, prepareSplits elapsed %d ms, scanTable elapsed %d ms",
                getTableElapsed, listPartitionsElapsed, prepareSplitElapsed, scanElapsed);
    }

    @Test
    public void scanHiveAllTypesPartition() throws Exception {
        Map<String, String> params = getParams();
        params.put("database_name", "test_las_table");
        params.put("table_name", "all_datatypes_no_partition_with_array_managed_hive");
        testScan(params);
    }

    @Test
    @Ignore
    public void scanHiveAllTypesNoPartition() throws Exception {
        Map<String, String> params = getParams();
        params.put("database_name", "dwd");
        params.put("table_name", "dwd_dtc_j3");
        testScan(params);
    }

    @Test
    @Ignore
    public void scanHiveDecimalBug() throws Exception {
        Map<String, String> params = getParams();
        params.put("database_name", "test_las_table");
        params.put("table_name", "all_datatypes_no_partition_with_array_managed_hive");
        params.put("required_columns", "decimal");
        testScan(params);
    }

    @Test
    @Ignore
    public void scanByteLakeTimestampAndArrayBug() throws Exception {
        Map<String, String> params = getParams();
        params.put("database_name", "test_las_table");
        params.put("table_name", "test_multi_partition_ddl");
        params.put("required_columns", "timestamp");
        testScan(params);
    }

    @Test
    @Ignore
    public void downloadFile() throws Exception {
        Map<String, String> params = getParams();
        params.put("database_name", "test_las_table");
        params.put("table_name", "all_datatypes_no_partition_with_array_managed_hive");
        LasMetaClient client = new LasMetaClient(params);
        Table hiveTable = client.getTableImpl();
        System.out.println(hiveTable);
        System.out.println(client.getColumnNames());
        System.out.println(client.getColumnTypes());
        client.downloadFile(hiveTable.getSd().getLocation(), "/tmp/test_las_table");
    }

}
