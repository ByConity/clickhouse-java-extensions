package org.byconity.iceberg.demo;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.byconity.iceberg.params.IcebergParams;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IcebergHiveDemo {
    public static void main(String[] args) throws IOException {
        try (HiveCatalog hiveCatalog = new HiveCatalog()) {
            hiveCatalog.initialize("IcebergHiveDemo",
                    ImmutableMap.of("uri", "thrift://10.146.43.237:9083", "warehouse",
                            "hdfs://10.146.43.237:12000/user/hive/warehouse"));
            List<Namespace> namespaces = hiveCatalog.listNamespaces();
            for (Namespace namespace : namespaces) {
                System.out.println(namespace);

                List<TableIdentifier> tableIdentifiers = hiveCatalog.listTables(namespace);
                for (TableIdentifier tableIdentifier : tableIdentifiers) {
                    System.out.printf("\t%s\n", tableIdentifier);
                }
            }
        }
    }

    private static final class Test {
        private static final Gson GSON = new GsonBuilder().create();

        public static void main(String[] args) throws Throwable {
            Map<String, String> map = Maps.newHashMap();

            map.put("metastoreType", "hive");
            map.put("uri", "thrift://10.146.43.237:9083");
            map.put("warehouse", "hdfs://10.146.43.237:12000/user/hive/warehouse");

            IcebergParams icebergMetaClient = IcebergParams.parseFrom(GSON.toJson(map));
            Catalog catalog = icebergMetaClient.getCatalog();
            List<Namespace> namespaces = ((SupportsNamespaces) catalog).listNamespaces();
            System.out.println(namespaces);

            List<TableIdentifier> tables = catalog.listTables(Namespace.of("iceberg_trino_demo"));
            System.out.println(tables);

            Table table = catalog.loadTable(TableIdentifier.of("iceberg_trino_demo", "test_table"));
            System.out.println(table);
        }
    }
}
