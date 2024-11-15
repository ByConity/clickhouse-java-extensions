package org.byconity.hudi.reader;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.byconity.common.ColumnType;
import org.byconity.common.ColumnValue;
import org.byconity.common.loader.ThreadContextClassLoader;
import org.byconity.common.reader.ArrowBatchReader;
import org.byconity.hudi.InputFormatColumnValue;

import java.io.IOException;

public class InputFormatArrowReader extends ArrowBatchReader {

    private final ClassLoader classLoader = getClass().getClassLoader();
    private final RecordReader<NullWritable, ArrayWritable> reader;
    private final StructObjectInspector rowInspector;
    private final StructField[] requiredStructFields;
    private final Deserializer deserializer;

    public InputFormatArrowReader(BufferAllocator allocator, int fetchSize,
            ColumnType[] requiredTypes, String[] requiredFields,
            RecordReader<NullWritable, ArrayWritable> reader, StructObjectInspector rowInspector,
            StructField[] requiredStructFields, Deserializer deserializer) {
        super(allocator, fetchSize, requiredTypes, requiredFields);
        this.reader = reader;
        this.rowInspector = rowInspector;
        this.requiredStructFields = requiredStructFields;
        this.deserializer = deserializer;
    }

    @Override
    protected int readArrowBatch() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            try {
                NullWritable key = reader.createKey();
                ArrayWritable value = reader.createValue();

                int numRows = 0;
                for (; numRows < getFetchSize(); numRows++) {
                    if (!reader.next(key, value)) {
                        break;
                    }
                    Object row = deserializer.deserialize(value);
                    for (int i = 0; i < requiredStructFields.length; i++) {
                        Object fieldData =
                                rowInspector.getStructFieldData(row, requiredStructFields[i]);
                        ObjectInspector inspector =
                                requiredStructFields[i].getFieldObjectInspector();
                        ColumnValue columnValue = new InputFormatColumnValue(fieldData, inspector);
                        appendValue(numRows, i, columnValue);
                    }
                }

                return numRows;
            } catch (Exception e) {
                throw new IOException("Failed to read next batch. " + e.getMessage(), e);
            }
        }
    }

    @Override
    protected void closeReadSource() throws IOException {
        reader.close();
    }
}
