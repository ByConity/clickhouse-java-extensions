package org.byconity.paimon.reader;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.InternalRowUtils;
import org.byconity.common.ColumnType;
import org.byconity.common.ColumnValue;
import org.byconity.common.loader.ThreadContextClassLoader;
import org.byconity.common.reader.ArrowBatchReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PaimonArrowReader extends ArrowBatchReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaimonArrowReader.class);

    private final ClassLoader classLoader = getClass().getClassLoader();
    private final DataType[] logicalTypes;
    private final RecordReaderIterator<InternalRow> iterator;
    private long totalTimeUsage = 0;
    private long totalReadNums = 0;

    public PaimonArrowReader(BufferAllocator allocator, int fetchSize, ColumnType[] requiredTypes,
            String[] requiredFields, DataType[] logicalTypes,
            RecordReaderIterator<InternalRow> iterator) {
        super(allocator, fetchSize, requiredTypes, requiredFields);
        this.logicalTypes = logicalTypes;
        this.iterator = iterator;
    }

    @Override
    protected int readArrowBatch() throws IOException {
        long start = System.currentTimeMillis();
        int numRows = 0;
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            while (iterator.hasNext() && numRows < fetchSize) {
                InternalRow row = iterator.next();
                if (row == null) {
                    break;
                }
                for (int i = 0; i < requiredFields.length; i++) {
                    Object fieldData = InternalRowUtils.get(row, i, logicalTypes[i]);
                    ColumnValue fieldValue = new PaimonColumnValue(fieldData, logicalTypes[i]);
                    appendValue(numRows, i, fieldValue);
                }
                numRows++;
            }
            return numRows;
        } catch (Exception e) {
            LOGGER.error("Paimon failed to read next batch, error={}", e.getMessage(), e);
            throw new IOException("Paimon failed to read next batch. " + e.getMessage(), e);
        } finally {
            totalReadNums += numRows;
            long useTime = System.currentTimeMillis() - start;
            LOGGER.debug("readArrowBatch numRows:{}, cost: {}ms", numRows, useTime);
            totalTimeUsage += useTime;
        }
    }

    @Override
    protected void closeReadSource() throws IOException {
        try {
            iterator.close();
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            LOGGER.info("totalTimeUsage: {}ms, totalReadNum: {}", totalTimeUsage, totalReadNums);
        }
    }
}
