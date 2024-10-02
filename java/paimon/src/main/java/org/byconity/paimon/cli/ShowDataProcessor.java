package org.byconity.paimon.cli;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.*;
import org.byconity.paimon.util.TimeUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ShowDataProcessor extends ActionProcessor {
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

    public ShowDataProcessor(Catalog catalog, String database, String table,
            Set<String> fieldNames) {
        super(catalog);
        this.database = database;
        this.table = table;
        this.fieldNames = fieldNames;
    }

    @Override
    protected void doProcess() throws Exception {
        Identifier identifier = Identifier.create(database, table);
        if (!catalog.tableExists(identifier)) {
            System.out.printf("Table '%s.%s' does not exist.\n", database, table);
            return;
        }

        Table paimonTable = catalog.getTable(identifier);
        RowType rowType = paimonTable.rowType();

        ReadBuilder readBuilder = paimonTable.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();

        long rowCount = 0;
        printHeader(rowType);

        TableRead read = readBuilder.newRead();
        try (RecordReader<InternalRow> reader = read.createReader(splits)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow row;
                while ((row = batch.next()) != null) {
                    System.out.printf("%d: %s\n", ++rowCount, stringify(rowType, row));
                    if (rowCount % 64 == 0) {
                        System.out.print("Press any key to continue...");
                        System.in.read();
                        printHeader(rowType);
                    }
                }
                batch.releaseBatch();
            }
        }
    }

    private void printHeader(RowType rowType) {
        StringBuilder buffer = new StringBuilder();
        boolean visitedFirstField = false;
        for (int i = 0; i < rowType.getFields().size(); i++) {
            DataField dataField = rowType.getFields().get(i);
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

    private String stringify(RowType rowType, InternalRow row) {
        StringBuilder buffer = new StringBuilder();
        boolean visitedFirstField = false;
        for (int i = 0; i < rowType.getFields().size(); i++) {
            DataField dataField = rowType.getFields().get(i);
            String name = dataField.name();
            DataType type = dataField.type();
            if (!fieldNames.isEmpty() && !fieldNames.contains(name)) {
                continue;
            }
            if (visitedFirstField) {
                buffer.append(", ");
            }
            visitedFirstField = true;
            if (row.isNullAt(i)) {
                buffer.append("NULL");
                continue;
            }
            switch (type.getTypeRoot()) {
                case BOOLEAN: {
                    buffer.append(row.getBoolean(i));
                    break;
                }
                case TINYINT: {
                    buffer.append(row.getByte(i));
                    break;
                }
                case SMALLINT: {
                    buffer.append(row.getShort(i));
                    break;
                }
                case INTEGER: {
                    buffer.append(row.getInt(i));
                    break;
                }
                case BIGINT: {
                    buffer.append(row.getLong(i));
                    break;
                }
                case FLOAT: {
                    buffer.append(row.getFloat(i));
                    break;
                }
                case DOUBLE: {
                    buffer.append(row.getDouble(i));
                    break;
                }
                case DATE: {
                    LocalDate localDate = LocalDate.ofEpochDay(row.getInt(i));
                    buffer.append(localDate);
                    break;
                }
                case TIMESTAMP_WITHOUT_TIME_ZONE: {
                    int precision = ((TimestampType) type).getPrecision();
                    Timestamp timestamp = row.getTimestamp(i, precision);
                    buffer.append(TimeUtils.DATETIME_PATTERNS.get(precision).format(LocalDateTime
                            .ofInstant(timestamp.toInstant(), ZoneId.systemDefault())));
                    break;
                }
                case CHAR:
                case VARCHAR: {
                    buffer.append('"').append(row.getString(i)).append('"');
                    break;
                }
                case BINARY:
                case VARBINARY: {
                    buffer.append('"').append(new String(row.getBinary(i))).append('"');
                    break;
                }
                case DECIMAL: {
                    int precision = ((DecimalType) type).getPrecision();
                    int scale = ((DecimalType) type).getScale();
                    buffer.append(row.getDecimal(i, precision, scale).toBigDecimal());
                    break;
                }
                case ARRAY: {
                    buffer.append("<array>");
                    break;
                }
                case MAP: {
                    buffer.append("<map>");
                    break;
                }
                case ROW: {
                    buffer.append("<row>");
                    break;
                }
            }
        }
        return buffer.toString();
    }
}
