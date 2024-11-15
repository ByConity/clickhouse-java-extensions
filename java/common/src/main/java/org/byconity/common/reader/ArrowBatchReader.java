package org.byconity.common.reader;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.byconity.common.ArrowColumnVector;
import org.byconity.common.ArrowSchemaUtils;
import org.byconity.common.ColumnType;
import org.byconity.common.ColumnValue;
import org.byconity.common.WritableColumnVector;

import java.io.IOException;


public abstract class ArrowBatchReader extends ArrowReader {
    protected int fetchSize;
    protected ColumnType[] requiredTypes;
    protected String[] requiredFields;
    protected WritableColumnVector[] columnVectors;

    public ArrowBatchReader(BufferAllocator allocator, int fetchSize, ColumnType[] requiredTypes,
            String[] requiredFields) {
        super(allocator);
        this.fetchSize = fetchSize;
        this.requiredTypes = requiredTypes;
        this.requiredFields = requiredFields;
    }

    protected int getFetchSize() {
        return fetchSize;
    }

    protected abstract int readArrowBatch() throws IOException;

    @Override
    public boolean loadNextBatch() throws IOException {
        prepareLoadNextBatch();

        if (columnVectors == null) {
            VectorSchemaRoot root = getVectorSchemaRoot();
            columnVectors = new WritableColumnVector[requiredTypes.length];
            for (int i = 0; i < columnVectors.length; i++) {
                columnVectors[i] =
                        new ArrowColumnVector(requiredTypes[i], fetchSize, root.getVector(i));
            }
        } else {
            for (WritableColumnVector column : columnVectors) {
                column.reset();
            }
        }

        int numRows = readArrowBatch();
        VectorSchemaRoot root = getVectorSchemaRoot();
        root.setRowCount(numRows);

        if (numRows <= 0) {
            return false;
        }

        VectorUnloader unloader = new VectorUnloader(root);
        ArrowRecordBatch nextBatch = unloader.getRecordBatch();
        loadRecordBatch(nextBatch);
        return true;
    }

    protected void appendValue(int rowIdx, int colIdx, ColumnValue value) {
        columnVectors[colIdx].putValue(rowIdx, value);
    }

    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {}

    @Override
    protected Schema readSchema() throws IOException {
        return ArrowSchemaUtils.toArrowSchema(requiredTypes, requiredFields);
    }
}
