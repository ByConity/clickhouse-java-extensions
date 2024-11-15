package org.byconity.common.metaclient;

import java.util.Map;

public interface MetaClient {
    default String getName() {
        return getClass().getSimpleName();
    }

    default byte[] getTable() throws Exception {
        throw new UnsupportedOperationException(
                String.format("'%s' for '%s' is not supported", getName(), "getTable"));
    }

    default byte[] getTableByName(String database, String table) throws Exception {
        throw new UnsupportedOperationException(
                String.format("'%s' for '%s' is not supported", getName(), "getTableByName"));
    }

    default byte[] getPartitionPaths() throws Exception {
        throw new UnsupportedOperationException(
                String.format("'%s' for '%s' is not supported", getName(), "getPartitionPaths"));
    }

    default byte[] getFilesInPartition(byte[] param) throws Exception {
        throw new UnsupportedOperationException(
                String.format("'%s' for '%s' is not supported", getName(), "getFilesInPartition"));
    }

    default byte[] getDatabase(String database) throws Exception {
        throw new UnsupportedOperationException(
                String.format("'%s' for '%s' is not supported", getName(), "getDatabase"));
    }

    default byte[] getPartitionsByFilter(String database, String table, String filter,
            short maxParts) throws Exception {
        throw new UnsupportedOperationException(String.format("'%s' for '%s' is not supported",
                getName(), "getPartitionsByFilter"));
    }

    default String[] listDatabases() throws Exception {
        throw new UnsupportedOperationException(
                String.format("'%s' for '%s' is not supported", getName(), "listDatabases"));
    }

    default String[] listTables(String database) throws Exception {
        throw new UnsupportedOperationException(
                String.format("'%s' for '%s' is not supported", getName(), "listTables"));
    }

    default boolean isTableExists(String database, String table) throws Exception {
        throw new UnsupportedOperationException(
                String.format("'%s' for '%s' is not supported", getName(), "isTableExists"));
    }

    default byte[] getPaimonSchema(Map<String, String> params) throws Exception {
        throw new UnsupportedOperationException(
                String.format("'%s' for '%s' is not supported", getName(), "getPaimonSchema"));
    }

    default Map<String, Object> getPaimonScanInfo(Map<String, String> params) throws Exception {
        throw new UnsupportedOperationException(
                String.format("'%s' for '%s' is not supported", getName(), "getPaimonSplits"));
    }

    default byte[] getIcebergSchema(Map<String, String> params) throws Exception {
        throw new UnsupportedOperationException(
                String.format("'%s' for '%s' is not supported", getName(), "getIcebergSchema"));
    }

    default Map<String, Object> getIcebergScanInfo(Map<String, String> params) throws Exception {
        throw new UnsupportedOperationException(
                String.format("'%s' for '%s' is not supported", getName(), "getIcebergScanInfo"));
    }
}
