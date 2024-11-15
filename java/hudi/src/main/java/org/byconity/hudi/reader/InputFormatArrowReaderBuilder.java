package org.byconity.hudi.reader;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.byconity.common.ColumnType;
import org.byconity.common.SelectedFields;
import org.byconity.common.reader.ArrowReaderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public abstract class InputFormatArrowReaderBuilder extends ArrowReaderBuilder {

    protected static final Logger LOGGER =
            LoggerFactory.getLogger(InputFormatArrowReaderBuilder.class);
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
    protected final Map<String, String> fsOptionsProps;

    protected final Map<String, String> params;

    protected static String getOrThrow(Map<String, String> map, String key) {
        return Optional.ofNullable(map.get(key))
                .orElseThrow(() -> new NoSuchElementException(key + " not found"));
    }

    public InputFormatArrowReaderBuilder(BufferAllocator allocator, Map<String, String> params) {
        this.allocator = allocator;
        this.fetchSize = Integer.parseInt(getOrThrow(params, "fetch_size"));
        this.hiveColumnNames = getOrThrow(params, "hive_column_names").split(",");
        this.hiveColumnTypes = getOrThrow(params, "hive_column_types").split("#");
        this.requiredFields = getOrThrow(params, "required_fields").split(",");
        this.nestedFields = params.getOrDefault("nested_fields", "").split(",");

        parseRequiredTypes();
        this.serde = getOrThrow(params, "serde");
        this.inputFormat = getOrThrow(params, "input_format");
        this.fsOptionsProps = params.keySet().stream().filter(name -> name.startsWith("fs."))
                .collect(Collectors.toMap(name -> name, params::get));
        this.params = params;
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

    public abstract InputSplit createInputSplit(JobConf jobConf) throws IOException;

    @SuppressWarnings("unchecked")
    private RecordReader<NullWritable, ArrayWritable> initReader(JobConf jobConf) throws Exception {
        InputFormat<?, ?> inputFormatClass = createInputFormat(jobConf, inputFormat);
        return (RecordReader<NullWritable, ArrayWritable>) inputFormatClass
                .getRecordReader(createInputSplit(jobConf), jobConf, Reporter.NULL);
    }

    protected Configuration makeConfiguration() throws IOException {
        return new Configuration();
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

        properties.setProperty("columns", String.join(",", this.hiveColumnNames));
        properties.setProperty("columns.types", String.join(",", this.hiveColumnTypes));
        properties.setProperty("serialization.lib", this.serde);
        fsOptionsProps.forEach(properties::setProperty);
        return properties;
    }

    @SuppressWarnings("unchecked")
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
        return "hiveColumnNames: " + Arrays.toString(hiveColumnNames) + "\n" + "hiveColumnTypes: "
                + Arrays.toString(hiveColumnTypes) + "\n" + "requiredFields: "
                + Arrays.toString(requiredFields) + "\n" + "serde: " + serde + "\n"
                + "inputFormat: " + inputFormat + "\n" + "fsOptionsProps: " + fsOptionsProps;
    }

    @Override
    public ArrowReader build() throws IOException {
        try {
            Properties properties = makeProperties();
            Configuration configuration = makeConfiguration();
            JobConf jobConf = makeJobConf(properties, configuration);
            RecordReader<NullWritable, ArrayWritable> reader = initReader(jobConf);
            Deserializer deserializer = getDeserializer(jobConf, properties, serde);
            StructObjectInspector rowInspector = getTableObjectInspector(deserializer);
            StructField[] requiredStructFields = new StructField[requiredFields.length];
            for (int i = 0; i < requiredFields.length; i++) {
                requiredStructFields[i] = rowInspector.getStructFieldRef(requiredFields[i]);
            }

            return new InputFormatArrowReader(allocator, fetchSize, requiredTypes, requiredFields,
                    reader, rowInspector, requiredStructFields, deserializer);
        } catch (Exception e) {
            LOGGER.error("Failed to create input split arrow reader. ", e);
            throw new IOException("Failed to create input split arrow reader. " + e.getMessage(),
                    e);
        }
    }

    @Override
    public BufferAllocator getAllocator() {
        return allocator;
    }
}
