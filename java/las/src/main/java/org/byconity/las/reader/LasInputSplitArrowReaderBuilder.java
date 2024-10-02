package org.byconity.las.reader;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.metastore.MetastoreHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.byconity.common.ColumnType;
import org.byconity.common.SelectedFields;
import org.byconity.common.loader.ThreadContextClassLoader;
import org.byconity.common.reader.ArrowReaderBuilder;
import org.byconity.las.OptimizedHadoopInputSplit;
import org.byconity.proto.HudiMeta;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;
import java.util.stream.Collectors;

import static org.byconity.las.LasMetaClient.isByteLakeV2Table;

public class LasInputSplitArrowReaderBuilder extends ArrowReaderBuilder {

    protected static final Logger LOG =
            LoggerFactory.getLogger(LasInputSplitArrowReaderBuilder.class);
    protected BufferAllocator allocator;
    protected final int fetchSize;
    protected final String[] hiveColumnNames;
    protected final String[] hiveColumnTypes;
    protected final String[] requiredFields;
    protected int[] requiredColumnIds;
    private ColumnType[] requiredTypes;
    private final String[] nestedFields;

    protected final String serde;
    protected final String inputFormat;
    protected final String fsOptionsProps;

    OptimizedHadoopInputSplit inputSplit;
    Configuration conf;

    protected static String getOrThrow(Map<String, String> map, String key) {
        return Optional.ofNullable(map.get(key))
                .orElseThrow(() -> new NoSuchElementException(key + " not found"));
    }

    public LasInputSplitArrowReaderBuilder(BufferAllocator allocator, Map<String, String> params) {
        this.allocator = allocator;
        this.fetchSize = Integer.parseInt(getOrThrow(params, "fetch_size"));
        this.hiveColumnNames = getOrThrow(params, "hive_column_names").split(",");
        this.hiveColumnTypes = getOrThrow(params, "hive_column_types").split("#");
        this.requiredFields = getOrThrow(params, "required_fields").split(",");
        this.nestedFields = params.getOrDefault("nested_fields", "").split(",");

        parseRequiredTypes();
        this.serde = getOrThrow(params, "serde");
        this.inputFormat = getOrThrow(params, "input_format");
        this.fsOptionsProps = params.get("fs_options_props");
        params.forEach((k, v) -> LOG.info("key=" + k + ", value=" + v));
    }

    private void parseRequiredTypes() {
        HashMap<String, Integer> hiveColumnNameToIndex = new HashMap<>();
        HashMap<String, String> hiveColumnNameToType = new HashMap<>();
        for (int i = 0; i < hiveColumnNames.length; i++) {
            hiveColumnNameToIndex.put(hiveColumnNames[i], i);
            hiveColumnNameToType.put(hiveColumnNames[i], hiveColumnTypes[i]);
        }

        requiredTypes = new ColumnType[requiredFields.length];
        requiredColumnIds = new int[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            requiredColumnIds[i] = hiveColumnNameToIndex.get(requiredFields[i]);
            String type = hiveColumnNameToType.get(requiredFields[i]);
            requiredTypes[i] = new ColumnType(requiredFields[i], type);
        }

        // prune fields
        SelectedFields ssf = new SelectedFields();
        for (String nestField : nestedFields) {
            ssf.addNestedPath(nestField);
        }
        for (int i = 0; i < requiredFields.length; i++) {
            ColumnType type = requiredTypes[i];
            String name = requiredFields[i];
            type.pruneOnField(ssf, name);
        }
    }

    private RecordReader<Writable, Writable> initReader(JobConf jobConf) throws Exception {
        InputFormat<?, ?> inputFormatClass = createInputFormat(jobConf, inputFormat);
        return (RecordReader<Writable, Writable>) inputFormatClass
                .getRecordReader(inputSplit.getHadoopInputSplit(), jobConf, Reporter.NULL);
    }

    protected JobConf makeJobConf(Properties properties, Configuration conf) {
        JobConf jobConf = new JobConf(conf);
        jobConf.setBoolean("hive.io.file.read.all.columns", false);
        properties.stringPropertyNames()
                .forEach(name -> jobConf.set(name, properties.getProperty(name)));
        return jobConf;
    }

    protected Properties makeProperties() {
        Properties properties = new Properties();
        properties.setProperty("hive.io.file.readcolumn.ids", Arrays.stream(this.requiredColumnIds)
                .mapToObj(String::valueOf).collect(Collectors.joining(",")));
        properties.setProperty("hive.io.file.readcolumn.names",
                String.join(",", this.requiredFields));
        // build `readNestedColumn.paths` spec.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < requiredFields.length; i++) {
            String name = requiredFields[i];
            ColumnType type = requiredTypes[i];
            type.buildNestedFieldsSpec(name, sb);
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
            properties.setProperty("hive.io.file.readNestedColumn.paths", sb.toString());
        }

        properties.setProperty("columns",
                Arrays.stream(this.hiveColumnNames).collect(Collectors.joining(",")));
        properties.setProperty("columns.types",
                Arrays.stream(this.hiveColumnTypes).collect(Collectors.joining(",")));
        properties.setProperty("serialization.lib", this.serde);
        parseFsOptions(fsOptionsProps, properties);
        return properties;
    }

    protected static void parseFsOptions(String fsOptionsProps, Properties properties) {
        if (fsOptionsProps == null) {
            return;
        }

        String[] props = fsOptionsProps.split("#");
        for (String prop : props) {
            String[] kv = prop.split("=");
            if (kv.length == 2) {
                properties.setProperty(kv[0], kv[1]);
            } else {
                LOG.warn("Invalid hive scanner fs options props argument: " + prop);
            }
        }
    }

    private InputFormat<?, ?> createInputFormat(Configuration conf, String inputFormat)
            throws Exception {
        Class<?> clazz = conf.getClassByName(inputFormat);
        Class<? extends InputFormat<?, ?>> cls =
                (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
        return ReflectionUtils.newInstance(cls, conf);
    }

    private Deserializer getDeserializer(Configuration configuration, Properties properties,
            String name) throws Exception {
        Class<? extends Deserializer> deserializerClass = Class
                .forName(name, true, JavaUtils.getClassLoader()).asSubclass(Deserializer.class);
        Deserializer deserializer = deserializerClass.getConstructor().newInstance();
        deserializer.initialize(configuration, properties);
        return deserializer;
    }

    private StructObjectInspector getTableObjectInspector(Deserializer deserializer)
            throws Exception {
        ObjectInspector inspector = deserializer.getObjectInspector();
        if (inspector.getCategory() != ObjectInspector.Category.STRUCT)
            throw new RuntimeException("expected STRUCT: " + inspector.getCategory());
        return (StructObjectInspector) inspector;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("hiveColumnNames: ");
        sb.append(Arrays.toString(hiveColumnNames));
        sb.append("\n");
        sb.append("hiveColumnTypes: ");
        sb.append(Arrays.toString(hiveColumnTypes));
        sb.append("\n");
        sb.append("requiredFields: ");
        sb.append(Arrays.toString(requiredFields));
        sb.append("\n");
        sb.append("serde: ");
        sb.append(serde);
        sb.append("\n");
        sb.append("inputFormat: ");
        sb.append(inputFormat);
        sb.append("\n");
        sb.append("fsOptionsProps: ");
        sb.append(fsOptionsProps);
        return sb.toString();
    }

    public static LasInputSplitArrowReaderBuilder create(byte[] raw)
            throws IOException, ClassNotFoundException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(
                LasInputSplitArrowReaderBuilder.class.getClassLoader())) {
            HudiMeta.LasInputSplitArrowReaderBuilderParams initParams =
                    HudiMeta.LasInputSplitArrowReaderBuilderParams.parseFrom(raw);
            byte[] inputSplitBytes = initParams.getInputSplit().getInputSplitBytes().toByteArray();
            ByteArrayInputStream bais = new ByteArrayInputStream(inputSplitBytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            OptimizedHadoopInputSplit inputSplit = (OptimizedHadoopInputSplit) ois.readObject();
            Map<String, String> params = initParams.getParams().getPropertiesList().stream()
                    .collect(Collectors.toMap(HudiMeta.Properties.KeyValue::getKey,
                            HudiMeta.Properties.KeyValue::getValue));

            BufferAllocator allocator = new RootAllocator();
            return new LasInputSplitArrowReaderBuilder(inputSplit, allocator, params);
        }
    }

    public LasInputSplitArrowReaderBuilder(OptimizedHadoopInputSplit inputSplit,
            BufferAllocator allocator, Map<String, String> params) {
        this.allocator = allocator;
        this.fetchSize = Integer.parseInt(getOrThrow(params, "fetch_size"));
        this.hiveColumnNames = getOrThrow(params, "hive_column_names").split(",");
        this.hiveColumnTypes = getOrThrow(params, "hive_column_types").split("#");
        this.requiredFields = getOrThrow(params, "required_fields").split(",");
        this.nestedFields = params.getOrDefault("nested_fields", "").split(",");

        parseRequiredTypes();
        this.serde = getOrThrow(params, "serde");
        this.inputFormat = getOrThrow(params, "input_format");
        this.fsOptionsProps = params.get("fs_options_props");
        params.forEach((k, v) -> LOG.debug("key=" + k + ", value=" + v));

        this.inputSplit = inputSplit;
        initConfiguration(params);
        inputSplit.initInputSplit(new JobConf(conf));
    }

    private void initConfiguration(Map<String, String> params) {
        conf = new Configuration();
        conf.set("fs.defaultFS", "lasfs:/");
        conf.set("fs.lasfs.impl", "com.volcengine.las.fs.LasFileSystem");
        conf.set("fs.lasfs.endpoint", params.getOrDefault("endpoint", "180.184.76.84:80"));
        conf.set("fs.lasfs.access.key", getOrThrow(params, "access_key"));
        conf.set("fs.lasfs.secret.key", getOrThrow(params, "secret_key"));
        conf.set("hive.metastore.uris",
                params.getOrDefault("metastore_uri", "thrift://111.62.122.160:48869"));
        conf.set("io.file.buffer.size", params.getOrDefault("buffer_size", "4194304"));

        conf.set("bms.cloud.region", params.getOrDefault("region", "cn-beijing"));
        conf.set("bms.cloud.accessKeyId", getOrThrow(params, "access_key"));
        conf.set("bms.cloud.secretAccessKey", getOrThrow(params, "secret_key"));
        conf.set("bms.cloud.service", "las");

        String thriftURI = params.getOrDefault("metastore_uri", "thrift://111.62.122.160:48869");
        thriftURI = thriftURI.replace("thrift://", "");
        String[] ipPort = thriftURI.split(":");
        if (ipPort.length != 2)
            throw new RuntimeException("bad thrift uri " + thriftURI);

        conf.set(HiveConf.ConfVars.HIVE_CLIENT_PUBLIC_CLOUD_SERVER_PLB_IP.varname, ipPort[0]);
        conf.set(HiveConf.ConfVars.HIVE_CLIENT_PUBLIC_CLOUD_SERVER_PLB_PORT.varname, ipPort[1]);
        MetastoreHolder.setThreadLocalConf(new SerializableConfiguration(conf));

        if (isByteLakeV2Table(conf)) {
            conf.set("bytelake.read.v2.enabled", "true");
        }

        // jobConf.set("bms.cloud.sessionToken", "");
        // jobConf.set("bms.cloud.identityId", "");
        // jobConf.set("bms.cloud.identityType","");
    }

    Configuration makeConfiguration() {
        return conf;
    }

    ;

    @Override
    public ArrowReader build() throws IOException {
        int fieldIdx = 0;
        try {
            Properties properties = makeProperties();
            Configuration configuration = makeConfiguration();
            JobConf jobConf = makeJobConf(properties, configuration);
            RecordReader<Writable, Writable> reader = initReader(jobConf);
            Deserializer deserializer = getDeserializer(jobConf, properties, serde);
            StructObjectInspector rowInspector = getTableObjectInspector(deserializer);
            StructField[] requiredStructFields = new StructField[requiredFields.length];
            for (fieldIdx = 0; fieldIdx < requiredFields.length; fieldIdx++) {
                requiredStructFields[fieldIdx] =
                        rowInspector.getStructFieldRef(requiredFields[fieldIdx]);
            }

            return new LasArrowReader(allocator, fetchSize, requiredTypes, requiredFields, reader,
                    rowInspector, requiredStructFields, deserializer);
        } catch (Exception e) {
            LOG.error("Failed to create input split arrow reader on required field idx " + fieldIdx,
                    e);
            throw new IOException("Failed to create input split arrow reader on required field idx "
                    + fieldIdx + ". " + e.getMessage(), e);
        }
    }

    @Override
    public BufferAllocator getAllocator() {
        return allocator;
    }
}
