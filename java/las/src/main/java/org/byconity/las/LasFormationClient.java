package org.byconity.las;

import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.byconity.common.aop.DefaultProxy;
import org.byconity.common.loader.ThreadContextClassLoader;
import org.byconity.common.metaclient.MetaClient;
import org.byconity.proto.HudiMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;


public class LasFormationClient implements MetaClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(LasFormationClient.class);
    // private IMetaStoreClient client;

    // LasFormation meta is like catalog-db-tbl.
    private String lfCatalog;
    private MetaStoreClientPool clientPool;


    public LasFormationClient(Map<String, String> params) throws MetaException {
        // client = HiveMetaStoreClient.newSynchronizedClient( new HiveMetaStoreClient(hiveConf));
        // client.setService("catalog_service");
        // client.setRegion(params.getOrDefault("region", "cn-beijing"));
        // client.setAccessKeyIdAndSecretAccessKey(getOrThrow(params, "access_key"),
        // getOrThrow(params, "secret_key"));
        lfCatalog = params.getOrDefault("lf.metastore.catalog", "");
        clientPool = new MetaStoreClientPool(1, 5, params);
    }

    private String translateDatabaseName(String db) {
        // LasFormation supports multiple catalog and the db under lfCatalog shall be visited using
        // db name @lfCatalog#db
        if (!lfCatalog.isEmpty() && !db.startsWith("@")) {
            return String.format("@%s#%s", lfCatalog, db);
        } else {
            return db;
        }
    }

    private static String getOrThrow(Map<String, String> map, String key) {
        return Optional.ofNullable(map.get(key))
                .orElseThrow(() -> new NoSuchElementException(key + " not found"));
    }

    public static LasFormationClient create(byte[] raw) throws Exception {
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(LasFormationClient.class.getClassLoader())) {
            HudiMeta.HudiMetaClientParams pb_params = HudiMeta.HudiMetaClientParams.parseFrom(raw);
            Map<String, String> params = pb_params.getProperties().getPropertiesList().stream()
                    .collect(Collectors.toMap(HudiMeta.Properties.KeyValue::getKey,
                            HudiMeta.Properties.KeyValue::getValue));

            return DefaultProxy.wrap(LasFormationClient.class, new Class[] {Map.class},
                    new Object[] {params});
        }
    }

    @Override
    protected void finalize() throws Throwable {
        LOGGER.debug("LasFormationClient is being garbage collected");
        super.finalize();
    }

    public Table getTableImpl(String database, String table) throws TException {
        try (MetaStoreClientPool.MetaStoreClient client = clientPool.getClient()) {
            return client.getHiveClient().getTable(translateDatabaseName(database), table);
        }
    }

    public <F extends TFieldIdEnum, T extends TBase<T, F>> byte[] thriftToBytes(T obj)
            throws TException {
        TMemoryBuffer memoryBuffer = new TMemoryBuffer(1024);
        TCompactProtocol compactProtocol = new TCompactProtocol(memoryBuffer);
        obj.write(compactProtocol);
        memoryBuffer.flush();
        try {
            return memoryBuffer.getArray();
        } finally {
            memoryBuffer.close();
        }
    }

    @Override
    public byte[] getTableByName(String database, String tableName) throws TException {
        Table table = this.getTableImpl(database, tableName);
        return thriftToBytes(table);
    }

    public Database getDatabaseImpl(String database) throws TException {
        try (MetaStoreClientPool.MetaStoreClient client = clientPool.getClient()) {

            Database db = client.getHiveClient().getDatabase(translateDatabaseName(database));

            return db;
        }
    }

    @Override
    public byte[] getDatabase(String database) throws TException {
        Database db = this.getDatabaseImpl(database);
        return thriftToBytes(db);
    }

    public List<Partition> getPartitionsByFilterImpl(String database, String table, String filter,
            short maxParts) throws TException {
        try (MetaStoreClientPool.MetaStoreClient client = clientPool.getClient()) {
            return client.getHiveClient().listPartitionsByFilter(translateDatabaseName(database),
                    table, filter, maxParts);
        }
    }

    @Override
    public byte[] getPartitionsByFilter(String database, String table, String filter,
            short maxParts) throws TException {
        List<Partition> partitions =
                this.getPartitionsByFilterImpl(database, table, filter, maxParts);
        return thriftToBytes(new PartitionListComposingSpec(partitions));
    }

    @Override
    public String[] listDatabases() throws TException {
        try (MetaStoreClientPool.MetaStoreClient client = clientPool.getClient()) {

            return client.getHiveClient().getDatabases(translateDatabaseName("*"))
                    .toArray(new String[0]);
        }
    }

    @Override
    public String[] listTables(String database) throws TException {
        try (MetaStoreClientPool.MetaStoreClient client = clientPool.getClient()) {
            return client.getHiveClient().getAllTables(translateDatabaseName(database))
                    .toArray(new String[0]);
        }
    }

    @Override
    public boolean isTableExists(String database, String table) throws TException {
        try (MetaStoreClientPool.MetaStoreClient client = clientPool.getClient()) {
            return client.getHiveClient().tableExists(translateDatabaseName(database), table);
        }
    }
}
