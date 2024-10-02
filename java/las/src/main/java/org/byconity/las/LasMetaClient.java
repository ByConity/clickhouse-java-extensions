package org.byconity.las;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.metastore.MetastoreHolder;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.byconity.common.aop.DefaultProxy;
import org.byconity.common.loader.ThreadContextClassLoader;
import org.byconity.common.metaclient.MetaClient;
import org.byconity.proto.HudiMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LasMetaClient implements MetaClient {
    private final ClassLoader classLoader = getClass().getClassLoader();
    String tableName;
    String databaseName;

    Map<String, String> params;
    HiveConf hiveConf;
    IMetaStoreClient metaStoreClient;
    Optional<Table> hiveTable = Optional.empty();
    protected static final Logger LOG = LoggerFactory.getLogger(LasMetaClient.class);

    public static LasMetaClient create(byte[] raw) throws Exception {
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(LasMetaClient.class.getClassLoader())) {
            HudiMeta.HudiMetaClientParams pb_params = HudiMeta.HudiMetaClientParams.parseFrom(raw);
            Map<String, String> params = pb_params.getProperties().getPropertiesList().stream()
                    .collect(Collectors.toMap(HudiMeta.Properties.KeyValue::getKey,
                            HudiMeta.Properties.KeyValue::getValue));

            return DefaultProxy.wrap(LasMetaClient.class, new Class[] {Map.class},
                    new Object[] {params});
        }
    }

    public LasMetaClient(Map<String, String> params) throws MetaException {
        this(params, "las");
    }

    public LasMetaClient(Map<String, String> params, String service) throws MetaException {
        this.params = params;
        tableName = getOrThrow(params, "table_name");
        databaseName = getOrThrow(params, "database_name");
        hiveConf = new HiveConf();
        hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_IS_PUBLIC_CLOUD, true);
        hiveConf.setBoolVar(HiveConf.ConfVars.METASTOREUSECONSUL, false);
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS,
                params.getOrDefault("metastore_uri", "thrift://111.62.122.160:48869"));
        hiveConf.setVar(HiveConf.ConfVars.HIVE_CLIENT_PUBLIC_CLOUD_TYPE, "las");
        hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT, "10000s");
        hiveConf.setVar(HiveConf.ConfVars.HIVE_CLIENT_LAS_REGION_NAME,
                params.getOrDefault("region", "cn-beijing"));

        String thriftURI = params.getOrDefault("metastore_uri", "thrift://111.62.122.160:48869");
        thriftURI = thriftURI.replace("thrift://", "");
        String[] ipPort = thriftURI.split(":");
        if (ipPort.length != 2)
            throw new RuntimeException("bad thrift uri " + thriftURI);

        hiveConf.setVar(HiveConf.ConfVars.HIVE_CLIENT_PUBLIC_CLOUD_SERVER_PLB_IP, ipPort[0]);
        hiveConf.setIntVar(HiveConf.ConfVars.HIVE_CLIENT_PUBLIC_CLOUD_SERVER_PLB_PORT,
                Integer.parseInt(ipPort[1]));
        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        hiveMetaStoreClient.setService(service);
        hiveMetaStoreClient.setRegion(params.getOrDefault("region", "cn-beijing"));
        hiveMetaStoreClient.setAccessKeyIdAndSecretAccessKey(getOrThrow(params, "access_key"),
                getOrThrow(params, "secret_key"));
        metaStoreClient = hiveMetaStoreClient;
    }

    private static String getOrThrow(Map<String, String> map, String key) {
        return Optional.ofNullable(map.get(key))
                .orElseThrow(() -> new NoSuchElementException(key + " not found"));
    }

    public synchronized Table getTableImpl() {
        return hiveTable.orElseGet(() -> {
            try {
                return metaStoreClient.getTable(databaseName, tableName);
            } catch (TException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public byte[] getTable() throws Exception {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Table table = getTableImpl();
            TMemoryBuffer memoryBuffer = new TMemoryBuffer(1024);
            TCompactProtocol compactProtocol = new TCompactProtocol(memoryBuffer);
            table.write(compactProtocol);
            memoryBuffer.flush();
            try {
                return memoryBuffer.getArray();
            } finally {
                memoryBuffer.close();
            }
        }
    }

    @Override
    public byte[] getPartitionPaths(String filter) throws Exception {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            List<Partition> partitions =
                    metaStoreClient.listPartitions(databaseName, tableName, (short) 1000);
            TMemoryBuffer memoryBuffer = new TMemoryBuffer(1024);
            TCompactProtocol compactProtocol = new TCompactProtocol(memoryBuffer);
            PartitionListComposingSpec partitionList = new PartitionListComposingSpec(partitions);
            partitionList.write(compactProtocol);
            memoryBuffer.flush();
            try {
                return memoryBuffer.getArray();
            } finally {
                memoryBuffer.close();
            }
        }
    }

    private InputFormat<?, ?> createInputFormat(Configuration conf, String inputFormat)
            throws ClassNotFoundException {
        Class<?> clazz = conf.getClassByName(inputFormat);
        Class<? extends InputFormat<?, ?>> cls =
                (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
        return ReflectionUtils.newInstance(cls, conf);
    }

    public static boolean isByteLakeV2Table(Configuration conf) {
        return conf.get("bytelake.version", "1.0").equalsIgnoreCase("2.0");
    }

    public List<OptimizedHadoopInputSplit> createSplits(int minNumSplits, int maxThreads,
            List<String> partitionPaths) throws Exception {
        Table hiveTable = getTableImpl();
        String hiveTableLocation = hiveTable.getSd().getLocation();
        Map<String, String> tableProperties = hiveTable.getParameters();

        JobConf jobConf = new JobConf(hiveConf);
        jobConf.set("fs.defaultFS", "lasfs:/");
        jobConf.set("fs.lasfs.impl", "com.volcengine.las.fs.LasFileSystem");
        jobConf.set("fs.lasfs.endpoint", params.getOrDefault("endpoint", "180.184.76.84:80"));
        jobConf.set("fs.lasfs.access.key", getOrThrow(params, "access_key"));
        jobConf.set("fs.lasfs.secret.key", getOrThrow(params, "secret_key"));
        jobConf.set("hive.metastore.uris",
                params.getOrDefault("metastore_uri", "thrift://111.62.122.160:48869"));
        jobConf.set("io.file.buffer.size", params.getOrDefault("buffer_size", "4194304"));

        jobConf.set("bms.cloud.region", params.getOrDefault("region", "cn-beijing"));
        jobConf.set("bms.cloud.accessKeyId", getOrThrow(params, "access_key"));
        jobConf.set("bms.cloud.secretAccessKey", getOrThrow(params, "secret_key"));
        jobConf.set("bms.cloud.service", "las");
        // jobConf.set("bms.cloud.sessionToken", "");
        // jobConf.set("bms.cloud.identityId", "");
        // jobConf.set("bms.cloud.identityType","");

        for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
            jobConf.set(entry.getKey(), entry.getValue());
        }

        if (isByteLakeV2Table(jobConf)) {
            // TODO: should I put true here?
            jobConf.set("bytelake.read.v2.enabled", "true");
        }

        if (partitionPaths == null || partitionPaths.isEmpty()) {
            partitionPaths = Collections.singletonList(hiveTableLocation);
        }

        class PartitionWithIndex {
            public final int index;
            public final String path;

            PartitionWithIndex(int index, String path) {
                this.index = index;
                this.path = path;
            }
        }

        List<String> finalPartitionPaths = partitionPaths;
        List<PartitionWithIndex> partitions = IntStream.range(0, partitionPaths.size())
                .mapToObj(index -> new PartitionWithIndex(index, finalPartitionPaths.get(index)))
                .collect(Collectors.toList());

        int numThreads = Math.min(maxThreads, partitions.size());
        LOG.info("Use " + numThreads + " threads to fetch splits");
        List<OptimizedHadoopInputSplit> inputSplits = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        try {
            CompletionService<List<OptimizedHadoopInputSplit>> completionService =
                    new ExecutorCompletionService<>(executorService);
            for (PartitionWithIndex partition : partitions) {
                completionService.submit(() -> {
                    JobConf localJobConf = new JobConf(jobConf);
                    localJobConf.set("mapreduce.input.fileinputformat.inputdir", partition.path);
                    MetastoreHolder.setThreadLocalConf(new SerializableConfiguration(localJobConf));
                    InputFormat<?, ?> mapredInputFormat =
                            createInputFormat(localJobConf, hiveTable.getSd().getInputFormat());
                    org.apache.hadoop.mapred.InputSplit[] hadoopInputSplits =
                            mapredInputFormat.getSplits(localJobConf, minNumSplits);
                    return Arrays.stream(hadoopInputSplits).map(hadoopInputSplit -> {
                        OptimizedHadoopInputSplit inputSplit =
                                new OptimizedHadoopInputSplit(hadoopInputSplit, partition.index);
                        inputSplit.prepareSerializedObject();
                        return inputSplit;
                    }).collect(Collectors.toList());
                });
            }

            for (int i = 0; i < partitions.size(); i++) {
                Future<List<OptimizedHadoopInputSplit>> fut = completionService.take();
                inputSplits.addAll(fut.get());
            }
        } finally {
            executorService.shutdown();
        }

        return inputSplits;
    }

    @Override
    public byte[] getFilesInPartition(byte[] params) throws Exception {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            HudiMeta.PartitionPaths partitionPaths = HudiMeta.PartitionPaths.parseFrom(params);
            int minSplitNum = partitionPaths.getSplitNum();
            int maxThreads = partitionPaths.getMaxThreads();
            List<OptimizedHadoopInputSplit> inputSplits =
                    createSplits(minSplitNum, maxThreads, partitionPaths.getPathsList());

            HudiMeta.InputSplits.Builder inputSplitsBuilder = HudiMeta.InputSplits.newBuilder();
            for (OptimizedHadoopInputSplit inputSplit : inputSplits) {
                inputSplitsBuilder.addInputSplits(inputSplit.serializeProtoBuf());
            }
            return inputSplitsBuilder.build().toByteArray();
        }
    }

    @Override
    public byte[] getDatabase(String database) throws Exception {
        throw new Exception("Not implemented");
    }

    @Override
    public byte[] getPartitionsByFilter(String database, String table, String filter,
            short maxParts) throws Exception {
        throw new Exception("Not implemented");
    }

    @Override
    public String[] listDatabases() throws Exception {
        throw new Exception("Not implemented");
    }

    @Override
    public String[] listTables(String database) throws Exception {
        throw new Exception("Not implemented");
    }

    public String getColumnNames() {
        Table table = getTableImpl();
        List<String> columns = table.getSd().getCols().stream().map(FieldSchema::getName)
                .collect(Collectors.toList());
        return String.join(",", columns);
    }

    public String getColumnTypes() {
        Table table = getTableImpl();
        List<String> types = table.getSd().getCols().stream().map(FieldSchema::getType)
                .collect(Collectors.toList());
        return String.join("#", types);
    }

    public void downloadFile(String remoteFile, String localPath) throws IOException {
        Configuration conf = new Configuration(hiveConf);
        conf.set("fs.defaultFS", "lasfs:/");
        conf.set("fs.lasfs.impl", "com.volcengine.las.fs.LasFileSystem");
        conf.set("fs.lasfs.endpoint", params.getOrDefault("endpoint", "180.184.76.84:80"));
        conf.set("fs.lasfs.access.key", getOrThrow(params, "access_key"));
        conf.set("fs.lasfs.secret.key", getOrThrow(params, "secret_key"));
        conf.set("io.file.buffer.size", params.getOrDefault("buffer_size", "4194304"));

        FileSystem lasFs = FileSystem.get(conf);
        Path downloadPath = new Path(remoteFile);
        Path destination = new Path(localPath);
        System.out.println("Download " + remoteFile + " to local path " + localPath);
        lasFs.copyToLocalFile(downloadPath, destination);
        lasFs.close();
    }
}
