package org.byconity.paimon.cli;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.*;
import org.apache.paimon.types.*;
import org.byconity.common.util.ExceptionUtils;
import org.byconity.common.util.TimeUtils;

import java.io.FileReader;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class LoadTableProcessor extends ActionProcessor {
    public static boolean printBatchInfo = true;
    private static final String COMMIT_USER = "paimoncli";
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
        Identifier identifier = Identifier.create(params.database, params.table);
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
        Identifier identifier = Identifier.create(params.database, params.table);
        FileStoreTable paimonTable = (FileStoreTable) catalog.getTable(identifier);

        List<DataField> fields = paimonTable.rowType().getFields();
        Preconditions.checkState(
                fields.stream()
                        .noneMatch(field -> field.type()
                                .getTypeRoot() == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                                || field.type().getTypeRoot() == DataTypeRoot.ARRAY
                                || field.type().getTypeRoot() == DataTypeRoot.MAP
                                || field.type().getTypeRoot() == DataTypeRoot.ROW
                                || field.type().getTypeRoot() == DataTypeRoot.MULTISET),
                "Not support types: TIMESTAMP_WITH_LOCAL_TIME_ZONE, ARRAY, MAP, ROW, MULTISET");

        List<ValueParser> valueParsers = fields.stream().map(field -> new ValueParser(field.type()))
                .collect(Collectors.toList());

        BlockingQueue<CSVRecord> queue = new ArrayBlockingQueue<>(params.batchSize);
        List<CommitMessage> allPreCommitMessages = Lists.newArrayList();
        FileReader reader = new FileReader(params.filePath);
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.builder()
                .setDelimiter(params.delimiter).setQuote(params.quote).build());
        List<BucketWriter> bucketWrites = Lists.newArrayList();
        BucketMode bucketMode = paimonTable.bucketMode();
        TableSchema tableSchema = paimonTable.schema();
        AtomicLong commitIdentifier = new AtomicLong();
        AtomicLong totalBatch = new AtomicLong();
        AtomicLong totalRowCount = new AtomicLong();
        AtomicReference<Exception> exception = new AtomicReference<>();
        AtomicBoolean exhausted = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(params.parallelism);
        StreamWriteBuilder writeBuilder =
                paimonTable.newStreamWriteBuilder().withCommitUser(COMMIT_USER);
        if (BucketMode.FIXED.equals(bucketMode)) {
            for (int bucketId = 0; bucketId < Integer
                    .parseInt(paimonTable.options().get("bucket")); bucketId++) {
                bucketWrites
                        .add(new BucketWriter(bucketId, writeBuilder.newWrite(), commitIdentifier,
                                allPreCommitMessages, params.batchSize, totalBatch, totalRowCount));
            }
        } else {
            bucketWrites.add(new BucketWriter(0, writeBuilder.newWrite(), commitIdentifier,
                    allPreCommitMessages, params.batchSize, totalBatch, totalRowCount));
        }

        ExecutorService executorService = Executors.newFixedThreadPool(params.parallelism);
        for (int i = 0; i < params.parallelism; i++) {
            executorService.submit(new CSVRecordReceiver(queue, valueParsers, tableSchema,
                    bucketMode, bucketWrites, exception, exhausted, latch));
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
        for (BucketWriter bucketWrite : bucketWrites) {
            bucketWrite.prepareCommit();
        }
        try (StreamTableCommit commit = writeBuilder.newCommit()) {
            commit.commit(commitIdentifier.getAndIncrement(), allPreCommitMessages);
        }

        System.out.printf("Load table '%s.%s' successfully, %d rows have been inserted.\n",
                params.database, params.table, totalRowCount.get());
    }

    private final static class CSVRecordReceiver implements Runnable {
        private final BlockingQueue<CSVRecord> queue;
        private final List<ValueParser> valueParsers;
        private final BucketMode bucketMode;
        private final List<BucketWriter> bucketWrites;
        private final FixedBucketRowKeyExtractor keyExtractor;
        private final AtomicReference<Exception> exception;
        private final AtomicBoolean nomoreInput;
        private final CountDownLatch latch;

        private CSVRecordReceiver(BlockingQueue<CSVRecord> queue, List<ValueParser> valueParsers,
                TableSchema tableSchema, BucketMode bucketMode, List<BucketWriter> bucketWrites,
                AtomicReference<Exception> exception, AtomicBoolean exhausted,
                CountDownLatch latch) {
            this.queue = queue;
            this.valueParsers = valueParsers;
            this.keyExtractor = new FixedBucketRowKeyExtractor(tableSchema);
            this.bucketMode = bucketMode;
            this.bucketWrites = bucketWrites;
            this.exception = exception;
            this.nomoreInput = exhausted;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    CSVRecord csvRecord = queue.poll(1, TimeUnit.SECONDS);
                    if (csvRecord == null) {
                        if (nomoreInput.get()) {
                            break;
                        }
                        continue;
                    }
                    process(csvRecord);
                }
            } catch (Exception e) {
                exception.compareAndSet(null, e);
            } finally {
                latch.countDown();
            }
        }

        private void process(CSVRecord csvRecord) throws Exception {
            List<Object> values = Lists.newArrayList();
            for (int i = 0; i < valueParsers.size(); i++) {
                values.add(valueParsers.get(i).parse(csvRecord.get(i)));
            }
            GenericRow row = GenericRow.of(values.toArray());
            if (BucketMode.FIXED.equals(bucketMode)) {
                keyExtractor.setRecord(row);
                bucketWrites.get(keyExtractor.bucket()).write(row);
            } else {
                bucketWrites.get(0).write(row);
            }
        }
    }


    private static final class BucketWriter {
        private final int bucketId;
        private final StreamTableWrite write;
        private final AtomicLong commitIdentifier;
        private final List<CommitMessage> allPreCommitMessages;
        private final int batchSize;
        private final AtomicLong totalBatch;
        private final AtomicLong totalRowCount;
        private long rowCount = 0;
        private boolean isCommitted = false;

        public BucketWriter(int bucketId, StreamTableWrite write, AtomicLong commitIdentifier,
                List<CommitMessage> allPreCommitMessages, int batchSize, AtomicLong totalBatch,
                AtomicLong totalRowCount) {
            this.bucketId = bucketId;
            this.write = write;
            this.commitIdentifier = commitIdentifier;
            this.allPreCommitMessages = allPreCommitMessages;
            this.batchSize = batchSize;
            this.totalBatch = totalBatch;
            this.totalRowCount = totalRowCount;
        }

        public synchronized void write(GenericRow row) throws Exception {
            write.write(row);
            ++rowCount;
            totalRowCount.incrementAndGet();
            if (rowCount % batchSize == 0) {
                long batchId = totalBatch.incrementAndGet();
                if (printBatchInfo) {
                    synchronized (System.out) {
                        System.out.printf(
                                "Batch %d has been inserted, bucket id: %d, total inserted rows: %d.\n",
                                batchId, bucketId, totalRowCount.get());
                    }
                }
            }
        }

        public synchronized void prepareCommit() throws Exception {
            if (isCommitted) {
                return;
            }
            List<CommitMessage> messages =
                    write.prepareCommit(true, commitIdentifier.getAndIncrement());
            synchronized (allPreCommitMessages) {
                allPreCommitMessages.addAll(messages);
            }
            isCommitted = true;
            if (printBatchInfo) {
                synchronized (System.out) {
                    System.out.printf("Bucket %d prepare commit, total inserted rows: %d.\n",
                            bucketId, totalRowCount.get());
                }
            }
        }
    }


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
        @SerializedName("batchSize")
        private int batchSize = 4096;
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


    private static class ValueParser {
        protected DataType type;

        protected ValueParser(DataType type) {
            this.type = type;
        }

        private Object parse(String value) {
            if (StringUtils.isBlank(value)) {
                return null;
            }
            return doParse(value);
        }

        private Object doParse(String value) {
            switch (type.getTypeRoot()) {
                case BOOLEAN:
                    return Boolean.parseBoolean(value);
                case TINYINT:
                    return Byte.parseByte(value);
                case SMALLINT:
                    return Short.parseShort(value);
                case INTEGER:
                    return Integer.parseInt(value);
                case BIGINT:
                    return Long.parseLong(value);
                case FLOAT:
                    return Float.parseFloat(value);
                case DOUBLE:
                    return Double.parseDouble(value);
                case DECIMAL: {
                    int precision = ((DecimalType) type).getPrecision();
                    int scale = ((DecimalType) type).getScale();
                    return Decimal.fromBigDecimal(new BigDecimal(value), precision, scale);
                }
                case DATE: {
                    for (DateTimeFormatter datePattern : TimeUtils.DATE_PATTERNS) {
                        try {
                            LocalDate localDate = LocalDate.parse(value, datePattern);
                            return (int) localDate.toEpochDay();
                        } catch (Exception e) {
                            // try next pattern
                        }
                    }
                    throw new UnsupportedOperationException(
                            String.format("Unsupported date pattern, %s", value));
                }
                case TIMESTAMP_WITHOUT_TIME_ZONE: {
                    int precision = ((TimestampType) type).getPrecision();
                    return Timestamp.fromInstant(
                            LocalDateTime.parse(value, TimeUtils.DATETIME_PATTERNS.get(precision))
                                    .toInstant(TimeUtils.DEFAULT_ZONE_OFFSET));
                }
                case CHAR:
                case VARCHAR:
                    return BinaryString.fromString(value);
                case BINARY:
                case VARBINARY:
                    return value.getBytes(Charset.defaultCharset());
                default:
                    throw new UnsupportedOperationException(
                            String.format("Unsupported load type: %s", type.getTypeRoot().name()));
            }
        }
    }
}
