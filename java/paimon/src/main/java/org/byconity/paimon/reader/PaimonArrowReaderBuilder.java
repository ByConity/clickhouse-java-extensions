package org.byconity.paimon.reader;

import com.google.common.base.Preconditions;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.byconity.common.ColumnType;
import org.byconity.common.SelectedFields;
import org.byconity.common.loader.ThreadContextClassLoader;
import org.byconity.common.reader.ArrowReaderBuilder;
import org.byconity.paimon.util.PaimonTypeUtils;
import org.byconity.paimon.util.PaimonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class PaimonArrowReaderBuilder extends ArrowReaderBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PaimonArrowReaderBuilder.class);

    protected final int fetchSize;
    private final BufferAllocator allocator = new RootAllocator();
    private final Table table;
    private final String[] requiredFields;
    private final String[] nestedFields;
    private final String[] encodedSplits;
    private final String encodedPredicate;
    private ColumnType[] requiredTypes;
    private DataType[] logicalTypes;

    private PaimonArrowReaderBuilder(JsonObject params) {
        Preconditions.checkState(params.has("encoded_table"));
        Preconditions.checkState(params.has("fetch_size"));
        Preconditions.checkState(params.has("required_fields"));
        Preconditions.checkState(params.has("encoded_splits"));

        table = PaimonUtils.decodeStringToObject(params.get("encoded_table").getAsString());
        fetchSize = params.get("fetch_size").getAsInt();
        requiredFields = params.get("required_fields").getAsString().split(",");
        if (params.has("nested_fields")) {
            nestedFields = params.get("nested_fields").getAsString().split(",");
        } else {
            nestedFields = new String[0];
        }
        encodedSplits = params.get("encoded_splits").getAsString().split(",");
        if (params.has("encoded_predicate")) {
            encodedPredicate = params.get("encoded_predicate").getAsString();
        } else {
            encodedPredicate = null;
        }

        LOGGER.debug("fetchSize={}, requiredFields={}, nestedFields={}", fetchSize, requiredFields,
                nestedFields);

        parseRequiredTypes();
    }

    public static PaimonArrowReaderBuilder create(byte[] raw) throws Throwable {
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(PaimonArrowReaderBuilder.class.getClassLoader())) {
            JsonObject params = JsonParser.parseString(new String(raw)).getAsJsonObject();
            return new PaimonArrowReaderBuilder(params);
        } catch (Throwable e) {
            LOGGER.error("Failed to create PaimonArrowReaderBuilder, error={}", e.getMessage(), e);
            throw e;
        }
    }

    private void parseRequiredTypes() {
        List<String> fieldNames = PaimonUtils.fieldNames(table.rowType());
        requiredTypes = new ColumnType[requiredFields.length];
        logicalTypes = new DataType[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            int index = fieldNames.indexOf(requiredFields[i]);
            if (index == -1) {
                throw new RuntimeException(String.format("Cannot find field %s in schema %s",
                        requiredFields[i], fieldNames));
            }
            DataType dataType = table.rowType().getTypeAt(index);
            String type = PaimonTypeUtils.fromPaimonType(dataType);
            requiredTypes[i] = new ColumnType(type);
            logicalTypes[i] = dataType;
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

    @Override
    public ArrowReader build() throws IOException {
        RowType rowType = table.rowType();
        List<String> fieldNames = PaimonUtils.fieldNames(rowType);

        int[] projected = Arrays.stream(requiredFields).mapToInt(fieldNames::indexOf).toArray();
        List<Split> splits = Arrays.stream(encodedSplits)
                .map(sequence -> (Split) PaimonUtils.decodeStringToObject(sequence))
                .collect(Collectors.toList());
        Predicate predicate = PaimonUtils.decodeStringToObject(encodedPredicate);

        RecordReader<InternalRow> reader = table.newReadBuilder().withProjection(projected)
                .withFilter(predicate).newRead().executeFilter().createReader(splits);

        RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader);

        return new PaimonArrowReader(allocator, fetchSize, requiredTypes, requiredFields,
                logicalTypes, iterator);
    }

    @Override
    public BufferAllocator getAllocator() {
        return allocator;
    }
}
