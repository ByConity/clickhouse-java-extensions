package org.byconity.iceberg.cli;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.types.Types;
import org.byconity.common.util.ExceptionUtils;
import org.byconity.common.util.TimeUtils;
import org.byconity.iceberg.writer.NativePartitionedWriter;

import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class LoadTableProcessor extends ActionProcessor {
    @SuppressWarnings("all")
    private static final class LoadTableParams {
        @SerializedName("database")
        private String database;
        @SerializedName("table")
        private String table;
        @SerializedName("filePath")
        private String filePath;
        @SerializedName("format")
        private String format = Format.CSV.name();

        // Params used for csv
        @SerializedName("targetFileSize")
        private int targetFileSize = 512 * 1024 * 1024;
        @SerializedName("parallelism")
        private int parallelism = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
        @SerializedName("delimiter")
        private char delimiter = ',';
        @SerializedName("quote")
        private char quote = '"';
    }


    enum Format {
        CSV,;

        static Format of(String str) {
            for (Format format : values()) {
                if (format.name().equalsIgnoreCase(str)) {
                    return format;
                }
            }

            return null;
        }
    }


    public static boolean printBatchInfo = true;
    private final LoadTableParams params;

    public static void process(Catalog catalog, String arg) throws Exception {
        LoadTableParams params;
        try {
            params = GSON.fromJson(arg, LoadTableParams.class);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Wrong arg format for action '%s', it should be json.",
                            ActionType.LOAD_TABLE.name()));
        }
        new LoadTableProcessor(catalog, params).doProcess();
    }

    private LoadTableProcessor(Catalog catalog, LoadTableParams params) {
        super(catalog);
        this.params = params;
    }

    @Override
    protected void doProcess() throws Exception {
        TableIdentifier identifier = TableIdentifier.of(params.database, params.table);
        if (!catalog.tableExists(identifier)) {
            System.out.printf("Table '%s.%s' does not exist.\n", params.database, params.table);
            return;
        }

        Format format = Format.of(params.format);

        if (format == Format.CSV) {
            loadCsv();
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported format type: %s", format));
        }
    }

    private void loadCsv() throws Exception {
        TableIdentifier identifier = TableIdentifier.of(params.database, params.table);
        Table icebergTable = catalog.loadTable(identifier);

        FileFormat fileFormat = FileFormat.fromString(icebergTable.properties()
                .getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name()));

        Schema schema = icebergTable.schema();
        PartitionSpec spec = icebergTable.spec();

        List<Types.NestedField> fields = schema.columns();
        Preconditions.checkState(
                fields.stream()
                        .noneMatch(field -> field.type().isListType() || field.type().isMapType()
                                || field.type().isStructType()),
                "Not support types: LIST, MAP, STRUCT");

        List<ValueParser> valueParsers =
                fields.stream().map(ValueParser::new).collect(Collectors.toList());

        BlockingQueue<CSVRecord> queue = new ArrayBlockingQueue<>(1024000);
        FileReader reader = new FileReader(params.filePath);
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.builder()
                .setDelimiter(params.delimiter).setQuote(params.quote).build());
        List<DataFile> allDataFiles = Lists.newArrayList();
        AtomicLong totalRowCount = new AtomicLong();
        AtomicReference<Exception> exception = new AtomicReference<>();
        AtomicBoolean exhausted = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(params.parallelism);

        ExecutorService executorService = Executors.newFixedThreadPool(params.parallelism);
        for (int i = 0; i < params.parallelism; i++) {
            executorService.submit(new Runnable() {
                BaseTaskWriter<Record> writer;

                @Override
                public void run() {
                    try {
                        initWriter();

                        while (true) {
                            CSVRecord csvRecord = queue.poll(1, TimeUnit.SECONDS);
                            if (csvRecord == null) {
                                if (exhausted.get()) {
                                    break;
                                }
                                continue;
                            }
                            process(csvRecord);
                        }

                        onFinished();
                    } catch (Exception e) {
                        exception.compareAndSet(null, e);
                    } finally {
                        latch.countDown();
                    }
                }

                private void initWriter() {
                    GenericAppenderFactory appenderFactory =
                            new GenericAppenderFactory(schema, spec);
                    // partitionId and taskId and operationId will be used as the file name, we only
                    // need to guarantee the combination is unique.
                    OutputFileFactory fileFactory = OutputFileFactory
                            .builderFor(icebergTable, 1, System.currentTimeMillis())
                            .format(fileFormat).operationId(UUID.randomUUID().toString()).build();
                    if (spec.isUnpartitioned()) {
                        writer = new UnpartitionedWriter<>(spec, fileFormat, appenderFactory,
                                fileFactory, icebergTable.io(), params.targetFileSize);
                    } else {
                        writer = new NativePartitionedWriter(spec, fileFormat, appenderFactory,
                                fileFactory, icebergTable.io(), params.targetFileSize, schema);
                    }
                }

                private void process(CSVRecord csvRecord) throws IOException {
                    totalRowCount.incrementAndGet();
                    GenericRecord record = GenericRecord.create(icebergTable.schema());
                    for (int i = 0; i < valueParsers.size(); i++) {
                        record.set(i, valueParsers.get(i).parse(csvRecord.get(i)));
                    }
                    writer.write(record);
                }

                private void onFinished() throws IOException {
                    writer.close();
                    synchronized (allDataFiles) {
                        allDataFiles.addAll(Arrays.asList(writer.dataFiles()));
                    }
                }
            });
        }

        for (CSVRecord record : csvParser) {
            queue.put(record);
        }

        exhausted.set(true);
        latch.await();
        executorService.shutdown();
        if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
            System.out.println("Load thread pool shutdown timeout.");
        }

        if (exception.get() != null) {
            ExceptionUtils.rethrow(exception.get());
        }

        // Commit
        AppendFiles appendFiles = icebergTable.newAppend();
        for (DataFile dataFile : allDataFiles) {
            appendFiles.appendFile(dataFile);
        }
        appendFiles.commit();

        System.out.printf("Load table '%s.%s' successfully, %d rows have been inserted.\n",
                params.database, params.table, totalRowCount.get());
    }

    private static class ValueParser {
        protected Types.NestedField field;

        protected ValueParser(Types.NestedField field) {
            this.field = field;
        }

        private Object parse(String value) {
            if (StringUtils.isBlank(value)) {
                return null;
            }
            return doParse(value);
        }

        private Object doParse(String value) {
            switch (field.type().typeId()) {
                case BOOLEAN:
                    return Boolean.parseBoolean(value);
                case INTEGER:
                    return Integer.parseInt(value);
                case LONG:
                    return Long.parseLong(value);
                case FLOAT:
                    return Float.parseFloat(value);
                case DOUBLE:
                    return Double.parseDouble(value);
                case DECIMAL: {
                    int precision = ((Types.DecimalType) field.type()).precision();
                    int scale = ((Types.DecimalType) field.type()).scale();
                    return new BigDecimal(value, new MathContext(precision, RoundingMode.HALF_UP))
                            .setScale(scale, RoundingMode.HALF_UP);
                }
                case DATE: {
                    for (DateTimeFormatter datePattern : TimeUtils.DATE_PATTERNS) {
                        try {
                            return LocalDate.parse(value, datePattern);
                        } catch (Exception e) {
                            // try next pattern
                        }
                    }
                    throw new UnsupportedOperationException(
                            String.format("Unsupported date pattern, %s", value));
                }
                case TIMESTAMP: {
                    for (DateTimeFormatter datetimePattern : TimeUtils.DATETIME_PATTERNS) {
                        try {
                            LocalDateTime localDateTime =
                                    LocalDateTime.parse(value, datetimePattern);
                            Instant instant =
                                    localDateTime.toInstant(TimeUtils.DEFAULT_ZONE_OFFSET);
                            return LocalDateTime.ofInstant(instant, TimeUtils.ZONE_UTC);
                        } catch (Exception e) {
                            // try next pattern
                        }
                    }
                    throw new UnsupportedOperationException(
                            String.format("Unsupported timestamp pattern, %s", value));
                }
                case STRING:
                    return value;
                case FIXED: {
                    Types.FixedType fixedType = (Types.FixedType) field.type();
                    byte[] sourceBytes = value.getBytes(Charset.defaultCharset());
                    byte[] bytes = new byte[fixedType.length()];
                    System.arraycopy(sourceBytes, 0, bytes, 0, sourceBytes.length);
                    return bytes;
                }
                case BINARY:
                    return ByteBuffer.wrap(value.getBytes(Charset.defaultCharset()));
                default:
                    throw new UnsupportedOperationException(
                            String.format("Unsupported load type: %s", field.type().typeId()));
            }
        }
    }
}
