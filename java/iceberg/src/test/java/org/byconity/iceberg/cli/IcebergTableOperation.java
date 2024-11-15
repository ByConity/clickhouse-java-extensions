/***
 * This part of code is inspired by Trino and Starrocks so you can find many similarities.
 */
package org.byconity.iceberg.cli;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.iceberg.*;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.byconity.common.writer.TableOperation;
import org.byconity.proto.LakeTypes;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class IcebergTableOperation implements TableOperation {


    private Table table;

    public IcebergTableOperation(Table table) {
        this.table = table;
    }

    public static String getIcebergRelativePartitionPath(String tableLocation,
            String partitionLocation) {
        tableLocation =
                tableLocation.endsWith("/") ? tableLocation.substring(0, tableLocation.length() - 1)
                        : tableLocation;
        String tableLocationWithData = tableLocation + "/data/";
        String path = partitionLocation.substring(tableLocationWithData.length());
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        return path;
    }

    public static PartitionData partitionDataFromPath(String relativePartitionPath,
            PartitionSpec spec) throws UnsupportedEncodingException {
        PartitionData data = new PartitionData(spec.fields().size());
        String[] partitions = relativePartitionPath.split("/", -1);
        List<PartitionField> partitionFields = spec.fields();

        for (int i = 0; i < partitions.length; i++) {
            PartitionField field = partitionFields.get(i);
            String[] parts = partitions[i].split("=", 2);
            Preconditions.checkArgument(
                    parts.length == 2 && parts[0] != null && field.name().equals(parts[0]),
                    "Invalid partition: %s", partitions[i]);

            org.apache.iceberg.types.Type sourceType = spec.partitionType().fields().get(i).type();
            // apply url decoding for string/fixed type
            if (sourceType.typeId() == Type.TypeID.STRING
                    || sourceType.typeId() == Type.TypeID.FIXED) {
                parts[1] = URLDecoder.decode(parts[1], StandardCharsets.UTF_8.toString());
            }

            if (parts[1].equals("null")) {
                data.set(i, null);
            } else {
                data.set(i, Conversions.fromPartitionString(sourceType, parts[1]));
            }
        }
        return data;
    }

    public void commit(LakeTypes.LakeSinkCommitInfos commitMsgs) {
        boolean isOverwrite = false;
        if (commitMsgs.getCommitInfosCount() != 0) {
            isOverwrite = commitMsgs.getCommitInfos(0).getIsOverwrite();
        }
        Transaction transaction = table.newTransaction();
        BatchWrite batchWrite = getBatchWrite(transaction, isOverwrite);


        List<LakeTypes.DataFileInfo> dataFiles = commitMsgs.getCommitInfosList().stream()
                .flatMap(x -> x.getDataFileInfoList().stream()).collect(Collectors.toList());
        PartitionSpec partitionSpec = table.spec();
        try {
            for (LakeTypes.DataFileInfo dataFile : dataFiles) {
                // TODO:: add metrics and split offsets from writer.
                DataFiles.Builder builder = DataFiles.builder(partitionSpec)
                        .withPath(dataFile.getPath()).withFormat(dataFile.getFormat())
                        .withRecordCount(dataFile.getRecordCount())
                        .withFileSizeInBytes(dataFile.getFileSizeInBytes());
                // .withSplitOffsets(dataFile.split_offsets);

                if (partitionSpec.isPartitioned()) {
                    String relativePartitionLocation = getIcebergRelativePartitionPath(
                            table.location(), dataFile.getPartitionPath());

                    PartitionData partitionData =
                            partitionDataFromPath(relativePartitionLocation, partitionSpec);
                    builder.withPartition(partitionData);
                }
                batchWrite.addFile(builder.build());
            }


            batchWrite.commit();
            transaction.commitTransaction();
        } catch (Exception e) {
            List<String> file_paths = dataFiles.stream().map(LakeTypes.DataFileInfo::getPath)
                    .collect(Collectors.toList());
            // TODO:: better handling. How to clean at best effort.
            for (String file_path : file_paths) {
                table.io().deleteFile(file_path);
            }
        }
    }

    public void abort(LakeTypes.LakeSinkCommitInfos commitMsgs) {
        List<LakeTypes.DataFileInfo> dataFiles = commitMsgs.getCommitInfosList().stream()
                .flatMap(x -> x.getDataFileInfoList().stream()).collect(Collectors.toList());
        List<String> file_paths = dataFiles.stream().map(LakeTypes.DataFileInfo::getPath)
                .collect(Collectors.toList());
        for (String file_path : file_paths) {
            table.io().deleteFile(file_path);
        }

    }

    private BatchWrite getBatchWrite(Transaction transaction, boolean isOverwrite) {
        return isOverwrite ? new DynamicOverwrite(transaction) : new Append(transaction);
    }

    @Override
    public void commit(byte[] commitMsgs) {
        LakeTypes.LakeSinkCommitInfos msgs;
        try {
            msgs = LakeTypes.LakeSinkCommitInfos.parseFrom(commitMsgs);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        commit(msgs);
    }

    @Override
    public void abort(byte[] commitMsgs) {
        LakeTypes.LakeSinkCommitInfos msgs;
        try {
            msgs = LakeTypes.LakeSinkCommitInfos.parseFrom(commitMsgs);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        abort(msgs);
    }


    interface BatchWrite {
        void addFile(DataFile file);

        void commit();

        void toBranch(String targetBranch);
    }


    static class Append implements BatchWrite {
        private AppendFiles append;

        public Append(Transaction txn) {
            append = txn.newAppend();
        }

        @Override
        public void addFile(DataFile file) {
            append.appendFile(file);
        }

        @Override
        public void commit() {
            append.commit();
        }

        @Override
        public void toBranch(String targetBranch) {
            append = append.toBranch(targetBranch);
        }
    }


    static class DynamicOverwrite implements BatchWrite {
        private ReplacePartitions replace;

        public DynamicOverwrite(Transaction txn) {
            replace = txn.newReplacePartitions();
        }

        @Override
        public void addFile(DataFile file) {
            replace.addFile(file);
        }

        @Override
        public void commit() {
            replace.commit();
        }

        @Override
        public void toBranch(String targetBranch) {
            replace = replace.toBranch(targetBranch);
        }
    }


    public static class PartitionData implements StructLike {
        private final Object[] values;

        private PartitionData(int size) {
            this.values = new Object[size];
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        public <T> T get(int pos, Class<T> javaClass) {
            return javaClass.cast(values[pos]);
        }

        @Override
        public <T> void set(int pos, T value) {
            if (value instanceof ByteBuffer) {
                ByteBuffer buffer = (ByteBuffer) value;
                byte[] bytes = new byte[buffer.remaining()];
                buffer.duplicate().get(bytes);
                values[pos] = bytes;
            } else {
                values[pos] = value;
            }
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            PartitionData that = (PartitionData) other;
            return Arrays.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }
}
