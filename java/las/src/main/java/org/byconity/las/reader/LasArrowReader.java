package org.byconity.las.reader;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.byconity.common.ColumnType;
import org.byconity.common.ColumnValue;
import org.byconity.common.loader.ThreadContextClassLoader;
import org.byconity.common.reader.ArrowBatchReader;
import org.byconity.las.LasColumnValue;

import java.io.IOException;

public class LasArrowReader extends ArrowBatchReader {

    private final ClassLoader classLoader = getClass().getClassLoader();
    private final RecordReader<Writable, Writable> reader;
    private final StructObjectInspector rowInspector;
    private final StructField[] requiredStructFields;
    private final Deserializer deserializer;

    Writable key;
    Writable value;

    public LasArrowReader(BufferAllocator allocator, int fetchSize, ColumnType[] types,
            String[] names, RecordReader<Writable, Writable> reader,
            StructObjectInspector rowInspector, StructField[] requiredStructFields,
            Deserializer deserializer) {
        super(allocator, fetchSize, types, names);
        this.reader = reader;
        this.rowInspector = rowInspector;
        this.requiredStructFields = requiredStructFields;
        this.deserializer = deserializer;
    }

    @Override
    protected int readArrowBatch() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            int numRows = 0;
            int requiredColIdx = 0;

            try {
                key = reader.createKey();
                value = reader.createValue();
                for (; numRows < getFetchSize(); numRows++) {
                    if (!reader.next(key, value)) {
                        break;
                    }
                    Object row = deserializer.deserialize(value);
                    for (requiredColIdx =
                            0; requiredColIdx < requiredStructFields.length; requiredColIdx++) {
                        Object fieldData = rowInspector.getStructFieldData(row,
                                requiredStructFields[requiredColIdx]);
                        ObjectInspector inspector =
                                requiredStructFields[requiredColIdx].getFieldObjectInspector();
                        ColumnValue columnValue = new LasColumnValue(fieldData, inspector);
                        appendValue(numRows, requiredColIdx, columnValue);
                    }
                }

                return numRows;
            } catch (Exception e) {
                throw new IOException("Failed to read next batch while reading " + numRows
                        + " rows of required column index " + requiredColIdx + ". Message "
                        + e.getMessage(), e);
            }
        }
    }

    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {
        reader.close();
    }
}
