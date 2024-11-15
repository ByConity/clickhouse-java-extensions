package org.byconity.iceberg.cli;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.iceberg.*;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.GenericAvroReader;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.byconity.common.util.TimeUtils;

import java.io.Closeable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

public class DeleteTableProcessor extends ActionProcessor {
    @SuppressWarnings("all")
    private static final class DeleteTableParams {
        @SerializedName("database")
        private String database;
        @SerializedName("table")
        private String table;
        @SerializedName("deleteType")
        private String deleteType;
        @SerializedName("isAnd")
        private boolean isAnd = true;
        @SerializedName("predicates")
        private List<String> predicates;
    }

    public static void process(Catalog catalog, String arg) throws Exception {
        DeleteTableParams params;
        try {
            params = GSON.fromJson(arg, DeleteTableParams.class);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Wrong arg format for action '%s', it should be json.",
                            ActionType.DELETE_TABLE.name()));
        }
        new DeleteTableProcessor(catalog, params).doProcess();
    }

    private final DeleteTableParams params;

    private DeleteTableProcessor(Catalog catalog, DeleteTableParams params) {
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

        FileContent fileContent = FileContent.valueOf(params.deleteType.toUpperCase());

        Table icebergTable = catalog.loadTable(identifier);

        if (CollectionUtils.isEmpty(params.predicates)) {
            throw new IllegalArgumentException("Predicates is empty");
        }

        List<Pair<String, Object>> columnNameAndValues =
                params.predicates.stream().map(predicate -> {
                    String[] segments = predicate.split("=");
                    String columnName = segments[0];
                    Object value = parseValue(icebergTable.schema(), columnName, segments[1]);
                    return Pair.of(columnName, value);
                }).collect(Collectors.toList());

        switch (fileContent) {
            case POSITION_DELETES:
                processPositionDelete(icebergTable, columnNameAndValues);
                break;
            case EQUALITY_DELETES:
                processEqualityDelete(icebergTable, columnNameAndValues);
                break;
            default:
                throw new IllegalArgumentException("Wrong content type");
        }

        System.out.printf("Delete table (%s) '%s.%s' successfully.\n", fileContent.name(),
                params.database, params.table);
    }

    private Object parseValue(Schema schema, String columnName, String valueString) {
        Type type = schema.findField(columnName).type();
        switch (type.typeId()) {
            case BOOLEAN:
                return Boolean.parseBoolean(valueString);
            case INTEGER:
                return Integer.parseInt(valueString);
            case LONG:
                return Long.parseLong(valueString);
            case FLOAT:
                return Float.parseFloat(valueString);
            case DOUBLE:
                return Double.parseDouble(valueString);
            case DECIMAL: {
                Types.DecimalType decimalType = (Types.DecimalType) type;
                int precision = decimalType.precision();
                int scale = decimalType.scale();
                return new BigDecimal(valueString, new MathContext(precision, RoundingMode.HALF_UP))
                        .setScale(scale, RoundingMode.HALF_UP);
            }
            case STRING:
                return valueString;
            case DATE:
                for (DateTimeFormatter datePattern : TimeUtils.DATE_PATTERNS) {
                    try {
                        return LocalDate.parse(valueString, datePattern);
                    } catch (Exception e) {
                        // try next pattern
                    }
                }
                throw new UnsupportedOperationException(
                        String.format("Unsupported date pattern, %s", valueString));
            case TIMESTAMP:
                for (DateTimeFormatter datetimePattern : TimeUtils.DATETIME_PATTERNS) {
                    try {
                        LocalDateTime localDateTime =
                                LocalDateTime.parse(valueString, datetimePattern);
                        Instant instant = localDateTime.toInstant(TimeUtils.DEFAULT_ZONE_OFFSET);
                        return LocalDateTime.ofInstant(instant, TimeUtils.ZONE_UTC);
                    } catch (Exception e) {
                        // try next pattern
                    }
                }
                throw new UnsupportedOperationException(
                        String.format("Unsupported timestamp pattern, %s", valueString));
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported type '%s' for delete", type.typeId().name()));
        }
    }

    private void processPositionDelete(Table icebergTable,
            List<Pair<String, Object>> columnNameAndValues) throws Exception {
        List<Triple<String, Long, Record>> deleteRecords = Lists.newArrayList();
        try (CloseableIterable<FileScanTask> fileScanTasks = icebergTable.newScan().planFiles()) {
            for (FileScanTask fileScanTask : fileScanTasks) {
                try (FileIO io = icebergTable.io()) {
                    String filePath = fileScanTask.file().path().toString();
                    InputFile inputFile = io.newInputFile(filePath);
                    CloseableIterable<Record> records;
                    switch (fileScanTask.file().format()) {
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
                                    .createReaderFunc(messageType -> GenericAvroReader
                                            .create(icebergTable.schema()))
                                    .build();
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                    String.format("Unsupported fileformat: %s",
                                            fileScanTask.file().format().name()));
                    }

                    long rowCount = 0;
                    for (Record record : records) {
                        boolean shouldDelete = params.isAnd;
                        for (Pair<String, Object> columnNameAndValue : columnNameAndValues) {
                            String columnName = columnNameAndValue.getKey();
                            Object columnValue = columnNameAndValue.getValue();
                            if (Objects.equals(record.getField(columnName), columnValue)) {
                                if (!params.isAnd) {
                                    shouldDelete = true;
                                    break;
                                }
                            } else {
                                if (params.isAnd) {
                                    shouldDelete = false;
                                    break;
                                }
                            }
                        }
                        if (shouldDelete) {
                            deleteRecords.add(Triple.of(filePath, rowCount, record));
                        }

                        rowCount++;
                    }

                    records.close();
                }
            }
        }

        if (CollectionUtils.isEmpty(deleteRecords)) {
            return;
        }

        try (FileIO io = icebergTable.io()) {
            OutputFile outputFile = io.newOutputFile(
                    icebergTable.location() + "/pos-deletes-" + UUID.randomUUID() + ".parquet");

            PositionDeleteWriter<Record> writer = Parquet.writeDeletes(outputFile)
                    .forTable(icebergTable).rowSchema(icebergTable.schema())
                    .createWriterFunc(GenericParquetWriter::buildWriter).overwrite()
                    .withSpec(icebergTable.spec()).buildPositionWriter();

            List<PositionDelete<Record>> records = Lists.newArrayList();
            for (Triple<String, Long, Record> triple : deleteRecords) {
                PositionDelete<Record> record = PositionDelete.create();
                record.set(triple.getLeft(), triple.getMiddle(), triple.getRight());
                records.add(record);
            }

            try (Closeable ignore = writer) {
                writer.write(records);
            }

            icebergTable.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
        }
    }

    void processEqualityDelete(Table icebergTable, List<Pair<String, Object>> columnNameAndValues)
            throws Exception {
        if (params.isAnd) {
            processEqualityDeleteForAndCompond(icebergTable, columnNameAndValues);
        } else {
            processEqualityDeleteForOrCompond(icebergTable, columnNameAndValues);
        }
    }

    void processEqualityDeleteForAndCompond(Table icebergTable,
            List<Pair<String, Object>> columnNameAndValues) throws Exception {
        List<Types.NestedField> fields =
                columnNameAndValues.stream().map(Pair::getKey).map(icebergTable.schema()::findField)
                        .map(Types.NestedField::asRequired).collect(Collectors.toList());
        List<Integer> fieldIds =
                fields.stream().map(Types.NestedField::fieldId).collect(Collectors.toList());
        long count = fields.stream().distinct().count();
        Preconditions.checkState(count == fieldIds.size(), "And predicates cannot be duplicated");
        Schema idEqDeleteSchema = new Schema(fields);

        try (FileIO io = icebergTable.io()) {
            OutputFile outputFile = io.newOutputFile(icebergTable.location() + "/equality-deletes-"
                    + UUID.randomUUID() + ".parquet");

            EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(outputFile)
                    .forTable(icebergTable).rowSchema(idEqDeleteSchema)
                    .createWriterFunc(GenericParquetWriter::buildWriter).overwrite()
                    .equalityFieldIds(fieldIds).buildEqualityWriter();

            try (Closeable ignore = writer) {
                Record deleteRecord = GenericRecord.create(idEqDeleteSchema);
                for (Pair<String, Object> columnNameAndValue : columnNameAndValues) {
                    String columnName = columnNameAndValue.getKey();
                    Object columnValue = columnNameAndValue.getValue();
                    deleteRecord.setField(columnName, columnValue);
                }
                writer.write(deleteRecord);
            }

            RowDelta rowDelta = icebergTable.newRowDelta();
            rowDelta.addDeletes(writer.toDeleteFile());
            rowDelta.commit();
        }
    }

    void processEqualityDeleteForOrCompond(Table icebergTable,
            List<Pair<String, Object>> columnNameAndValues) throws Exception {
        Map<String, Pair<Schema, List<Object>>> groups = Maps.newHashMap();
        for (Pair<String, Object> columnNameAndValue : columnNameAndValues) {
            String columnName = columnNameAndValue.getKey();
            Object columnValue = columnNameAndValue.getValue();
            Types.NestedField field = icebergTable.schema().findField(columnName).asRequired();
            if (!groups.containsKey(columnName)) {
                groups.put(columnName, Pair.of(new Schema(field), Lists.newArrayList()));
            }
            groups.get(columnName).getValue().add(columnValue);
        }

        for (Map.Entry<String, Pair<Schema, List<Object>>> entry : groups.entrySet()) {
            String columnName = entry.getKey();
            Schema schema = entry.getValue().getKey();
            List<Integer> fieldIds = schema.columns().stream().map(Types.NestedField::fieldId)
                    .collect(Collectors.toList());
            List<Object> columnValues = entry.getValue().getValue();

            try (FileIO io = icebergTable.io()) {
                OutputFile outputFile = io.newOutputFile(icebergTable.location()
                        + "/equality-deletes-" + UUID.randomUUID() + ".parquet");

                EqualityDeleteWriter<Record> writer =
                        Parquet.writeDeletes(outputFile).forTable(icebergTable).rowSchema(schema)
                                .createWriterFunc(GenericParquetWriter::buildWriter).overwrite()
                                .equalityFieldIds(fieldIds).buildEqualityWriter();

                try (Closeable ignore = writer) {
                    for (Object value : columnValues) {
                        Record deleteRecord = GenericRecord.create(schema);
                        deleteRecord.setField(columnName, value);
                        writer.write(deleteRecord);
                    }
                }

                RowDelta rowDelta = icebergTable.newRowDelta();
                rowDelta.addDeletes(writer.toDeleteFile());
                rowDelta.commit();
            }
        }
    }
}
