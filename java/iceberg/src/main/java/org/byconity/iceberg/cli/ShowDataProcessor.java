package org.byconity.iceberg.cli;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.*;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.GenericAvroReader;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.byconity.common.util.TimeUtils;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

public class ShowDataProcessor extends ActionProcessor {
    private final static Schema POSITIONAL_DELETE_SCHEMA =
            // Those field ids are reserved
            new Schema(Types.NestedField.required(2147483546, "file_path", Types.StringType.get()),
                    Types.NestedField.required(2147483545, "pos", Types.LongType.get()));

    private static final Field FIELD_EQUALITY_IDS;

    static {
        try {
            Class<?> clazz = Class.forName("org.apache.iceberg.BaseFile");
            FIELD_EQUALITY_IDS = clazz.getDeclaredField("equalityIds");
            FIELD_EQUALITY_IDS.setAccessible(true);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    private final String database;
    private final String table;
    private final Set<String> fieldNames;

    public static void process(Catalog catalog, String arg) throws Exception {
        String[] segments = arg.split("\\.");
        Preconditions.checkState(segments.length >= 2,
                String.format("Arg for action '%s' should be <database>.<table>[.{<field_list>}].",
                        ActionType.SHOW_DATA.name()));
        Set<String> fieldNames;
        if (segments.length > 2) {
            String fieldContent = segments[2].trim();
            Preconditions.checkState(fieldContent.startsWith("{") && fieldContent.endsWith("}"),
                    String.format(
                            "Arg for action '%s' should be <database>.<table>[.{<field_list>}].",
                            ActionType.SHOW_DATA.name()));
            fieldNames =
                    Arrays.stream(fieldContent.substring(1, fieldContent.length() - 1).split(","))
                            .map(String::trim).collect(Collectors.toSet());
        } else {
            fieldNames = Sets.newHashSet();
        }
        new ShowDataProcessor(catalog, segments[0], segments[1], fieldNames).doProcess();
    }

    private ShowDataProcessor(Catalog catalog, String database, String table,
            Set<String> fieldNames) {
        super(catalog);
        this.database = database;
        this.table = table;
        this.fieldNames = fieldNames;
    }

    @Override
    protected void doProcess() throws Exception {
        TableIdentifier identifier = TableIdentifier.of(database, table);
        if (!catalog.tableExists(identifier)) {
            System.out.printf("Table '%s.%s' does not exist.\n", database, table);
            return;
        }

        Table icebergTable = catalog.loadTable(identifier);
        Schema schema = icebergTable.schema();

        long totalRowCount = 0;
        printHeader(schema);

        try (CloseableIterable<FileScanTask> fileScanTasks = icebergTable.newScan().planFiles()) {
            for (FileScanTask fileScanTask : fileScanTasks) {
                List<DeleteFile> deletes = fileScanTask.deletes();
                // file name -> set of delete positions
                Map<String, Set<Long>> allDeletePositions = Maps.newHashMap();
                // Each item is a list is a pair of (column name, value)
                List<List<Pair<String, Object>>> allDeleteItems = Lists.newArrayList();
                if (CollectionUtils.isNotEmpty(deletes)) {
                    for (DeleteFile delete : deletes) {
                        switch (delete.content()) {
                            case POSITION_DELETES:
                                parsePositionDeleteFile(icebergTable, delete, allDeletePositions);
                                break;
                            case EQUALITY_DELETES:
                                parseEqualityDeleteFile(icebergTable, delete, allDeleteItems);
                                break;
                            default:
                                throw new IllegalStateException();
                        }
                    }
                }

                try (FileIO io = icebergTable.io()) {
                    String dataFilePath = fileScanTask.file().path().toString();
                    InputFile inputFile = io.newInputFile(dataFilePath);
                    Set<Long> deletePositions =
                            allDeletePositions.getOrDefault(dataFilePath, Sets.newHashSet());
                    CloseableIterable<Record> records = createRecordIterator(icebergTable,
                            inputFile, fileScanTask.file().format());
                    long rowCount = 0;
                    for (Record record : records) {
                        if (deletePositions.contains(rowCount++)) {
                            continue;
                        }

                        boolean findEqualityDeleteRecord = false;
                        for (List<Pair<String, Object>> deleteItem : allDeleteItems) {
                            boolean isItemMatched = true;
                            for (Pair<String, Object> columnNameAndValue : deleteItem) {
                                String columnName = columnNameAndValue.getKey();
                                Object columnValue = columnNameAndValue.getValue();
                                Preconditions.checkState(columnValue != null);

                                if (!Objects.equals(columnValue, record.getField(columnName))) {
                                    isItemMatched = false;
                                    break;
                                }
                            }
                            if (isItemMatched) {
                                findEqualityDeleteRecord = true;
                                break;
                            }
                        }

                        if (findEqualityDeleteRecord) {
                            continue;
                        }

                        System.out.printf("%d: %s\n", ++totalRowCount, stringify(schema, record));
                        if (totalRowCount % 64 == 0) {
                            System.out.print("Press any key to continue...");
                            Preconditions.checkState(System.in.read() != -1);
                            printHeader(schema);
                        }
                    }

                    records.close();
                }
            }
        }
    }

    private void parsePositionDeleteFile(Table icebergTable, DeleteFile delete,
            Map<String, Set<Long>> allDeletePositions) throws Exception {
        try (FileIO io = icebergTable.io()) {
            InputFile inputFile = io.newInputFile(delete.path().toString());
            try (CloseableIterable<Record> records =
                    Parquet.read(inputFile).project(POSITIONAL_DELETE_SCHEMA)
                            .createReaderFunc(messageType -> GenericParquetReaders
                                    .buildReader(POSITIONAL_DELETE_SCHEMA, messageType))
                            .build()) {

                for (Record record : records) {
                    String dataFilePath = (String) record.getField("file_path");
                    Long pos = (Long) record.getField("pos");
                    if (!allDeletePositions.containsKey(dataFilePath)) {
                        allDeletePositions.put(dataFilePath, Sets.newHashSet());
                    }
                    allDeletePositions.get(dataFilePath).add(pos);
                }
            }
        }
    }

    private void parseEqualityDeleteFile(Table icebergTable, DeleteFile delete,
            List<List<Pair<String, Object>>> allDeleteItems) throws Exception {
        Schema schema = icebergTable.schema();
        Map<Integer, String> idToName = schema.idToName();
        int[] equalityIds = (int[]) FIELD_EQUALITY_IDS.get(delete);
        List<Types.NestedField> fields = Lists.newArrayList();
        for (int equalityId : equalityIds) {
            fields.add(schema.findField(equalityId).asOptional());
        }
        Schema deleteSchema = new Schema(fields);
        try (FileIO io = icebergTable.io()) {
            InputFile inputFile = io.newInputFile(delete.path().toString());
            try (CloseableIterable<Record> records = Parquet.read(inputFile).project(deleteSchema)
                    .createReaderFunc(messageType -> GenericParquetReaders.buildReader(deleteSchema,
                            messageType))
                    .build()) {

                for (Record record : records) {
                    List<Pair<String, Object>> deleteItem = Lists.newArrayList();
                    for (int equalityId : equalityIds) {
                        String name = idToName.get(equalityId);
                        Object value = record.getField(name);
                        deleteItem.add(Pair.of(name, value));
                    }
                    allDeleteItems.add(deleteItem);
                }
            }
        }
    }

    private CloseableIterable<Record> createRecordIterator(Table icebergTable, InputFile inputFile,
            FileFormat format) {
        CloseableIterable<Record> records;
        switch (format) {
            case ORC:
                records = ORC.read(inputFile).project(icebergTable.schema())
                        .createReaderFunc(messageType -> GenericOrcReader
                                .buildReader(icebergTable.schema(), messageType))
                        .build();
                break;
            case PARQUET:
                records = Parquet.read(inputFile).project(icebergTable.schema())
                        .createReaderFunc(messageType -> GenericParquetReaders
                                .buildReader(icebergTable.schema(), messageType))
                        .build();
                break;
            case AVRO:
                records = Avro.read(inputFile).project(icebergTable.schema())
                        .createReaderFunc(
                                messageType -> GenericAvroReader.create(icebergTable.schema()))
                        .build();
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported fileformat: %s", format.name()));
        }
        return records;
    }

    private void printHeader(Schema schema) {
        StringBuilder buffer = new StringBuilder();
        boolean visitedFirstField = false;
        for (int i = 0; i < schema.columns().size(); i++) {
            Types.NestedField dataField = schema.columns().get(i);
            String name = dataField.name();
            if (!fieldNames.isEmpty() && !fieldNames.contains(name)) {
                continue;
            }
            if (visitedFirstField) {
                buffer.append(", ");
            }
            visitedFirstField = true;
            buffer.append(name);
        }
        System.out.println(buffer);
    }

    private String stringify(Schema schema, Record record) {
        StringBuilder buffer = new StringBuilder();
        boolean visitedFirstField = false;
        for (int i = 0; i < schema.columns().size(); i++) {
            Types.NestedField dataField = schema.columns().get(i);
            String name = dataField.name();
            if (!fieldNames.isEmpty() && !fieldNames.contains(name)) {
                continue;
            }
            if (visitedFirstField) {
                buffer.append(", ");
            }
            visitedFirstField = true;
            if (record.get(i) == null) {
                buffer.append("NULL");
                continue;
            }
            switch (dataField.type().typeId()) {
                case FIXED:
                    buffer.append(new String((byte[]) record.get(i)));
                    break;
                case BINARY:
                    ByteBuffer byteBuffer = ((ByteBuffer) record.get(i));
                    byte[] bytes = new byte[byteBuffer.remaining()];
                    byteBuffer.get(bytes);
                    buffer.append(new String(bytes));
                    break;
                case TIMESTAMP:
                    buffer.append(TimeUtils.DATETIME_PATTERNS.get(3)
                            .format(LocalDateTime.ofInstant(
                                    ((LocalDateTime) record.get(i)).toInstant(ZoneOffset.UTC),
                                    ZoneId.systemDefault())));
                    break;
                default:
                    buffer.append(record.get(i));
            }
        }
        return buffer.toString();
    }
}
