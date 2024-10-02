package org.byconity.paimon;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.byconity.common.aop.DefaultProxy;
import org.byconity.common.loader.ThreadContextClassLoader;
import org.byconity.common.metaclient.MetaClient;
import org.byconity.paimon.params.PaimonParams;
import org.byconity.paimon.util.PaimonSchemaUtils;
import org.byconity.paimon.util.PaimonUtils;
import org.byconity.paimon.util.RPNPredicateParser;
import org.byconity.proto.PaimonMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class PaimonMetaClient implements MetaClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(PaimonMetaClient.class);

    private static final Set<String> supportedNativeProtocols = Collections.unmodifiableSet(
            Sets.newHashSet("HDFS", "S3", "S3A", "COSN", "COS", "OBS", "OSS", "TOS"));
    private final PaimonParams params;
    private final Catalog catalog;

    public PaimonMetaClient(PaimonParams params) {
        this.params = params;
        this.catalog = params.getCatalog();
    }

    public static PaimonMetaClient create(byte[] bytes) throws Throwable {
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(PaimonMetaClient.class.getClassLoader())) {
            PaimonParams params = PaimonParams.parseFrom(new String(bytes));
            return DefaultProxy.wrap(PaimonMetaClient.class, new Class[] {PaimonParams.class},
                    new Object[] {params});
        } catch (Throwable e) {
            LOGGER.error("Failed to create PaimonMetaClient, error={}", e.getMessage(), e);
            throw e;
        } finally {
            LOGGER.debug("PaimonMetaClient create params: {}", new String(bytes));
        }
    }

    public PaimonParams getParams() {
        return params;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    @Override
    public String[] listDatabases() {
        return catalog.listDatabases().toArray(new String[0]);
    }

    @Override
    public String[] listTables(String database) throws Exception {
        return catalog.listTables(database).toArray(new String[0]);
    }

    @Override
    public boolean isTableExists(String database, String table) {
        return catalog.tableExists(Identifier.create(database, table));
    }

    @Override
    public byte[] getPaimonSchema(Map<String, String> params) throws Exception {
        String dbName = params.get("database");
        String tableName = params.get("table");
        Table table = catalog.getTable(Identifier.create(dbName, tableName));
        RowType rowType = table.rowType();
        PaimonMeta.Schema proto = PaimonSchemaUtils.toProto(rowType.getFields(),
                table.primaryKeys(), table.partitionKeys(), table.options());
        return proto.toByteArray();
    }

    /**
     * @param params Contains information like predicate to generate splits
     * @return res["encoded_table"] represents the serialized data of Table. res["encoded_splits"]
     *         represents the serialized data of Split
     */
    @Override
    public Map<String, Object> getPaimonScanInfo(Map<String, String> params) throws Exception {
        boolean forceJni = Boolean.parseBoolean(params.get("force_jni"));
        String dbName = params.get("database");
        String tableName = params.get("table");
        String[] requiredFields = null;
        if (params.containsKey("required_fields")) {
            requiredFields = params.get("required_fields").split(",");
        }
        String rpnPredicate = params.get("rpn_predicate");

        Table table = catalog.getTable(Identifier.create(dbName, tableName));

        int[] projected = null;
        if (requiredFields != null && requiredFields.length > 0) {
            RowType rowType = table.rowType();
            List<String> fieldNames = PaimonUtils.fieldNames(rowType);
            projected = Arrays.stream(requiredFields).mapToInt(fieldNames::indexOf).toArray();
        }
        Predicate predicate = RPNPredicateParser.parse(table, rpnPredicate);
        LOGGER.debug("database: {}, table: {}, requiredFields: {}, rpnPredicate: {}, predicate: {}",
                dbName, tableName, requiredFields, rpnPredicate, predicate);

        List<Split> splits = table.newReadBuilder().withFilter(predicate).withProjection(projected)
                .newScan().plan().splits();
        Pair<List<Split>, List<DataSplit>> pair = splitIntoJniAndNativeSplits(forceJni, splits);
        List<Split> jniSplits = pair.getLeft();
        List<DataSplit> nativeSplits = pair.getRight();

        byte[] encodedTable = PaimonUtils.encodeObjectToBytes(table);
        List<byte[]> encodedSplits = jniSplits.parallelStream()
                .map(PaimonUtils::encodeObjectToBytes).collect(Collectors.toList());
        Map<String, DataFileMeta> dataFiles =
                nativeSplits
                        .stream().map(
                                dataSplit -> dataSplit.dataFiles().stream()
                                        .collect(Collectors.toMap(
                                                file -> String.format("%s/%s",
                                                        dataSplit.bucketPath(), file.fileName()),
                                                file -> file)))
                        .flatMap(map -> map.entrySet().stream())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        List<byte[]> rawFiles = nativeSplits.stream().map(DataSplit::convertToRawFiles)
                .filter(Optional::isPresent).map(Optional::get).flatMap(List::stream)
                .map(rawFile -> rawFileToJsonString(rawFile, dataFiles.get(rawFile.path())))
                .map(String::getBytes).collect(Collectors.toList());

        byte[] encodedPredicate = PaimonUtils.encodeObjectToBytes(predicate);

        Map<String, Object> res = Maps.newHashMap();
        res.put("encoded_table", encodedTable);
        res.put("encoded_splits", encodedSplits);
        res.put("raw_files", rawFiles);
        if (encodedPredicate != null) {
            res.put("encoded_predicate", encodedPredicate);
        }
        return res;
    }

    private Pair<List<Split>, List<DataSplit>> splitIntoJniAndNativeSplits(boolean forceJni,
            List<Split> splits) {
        List<Split> jniSplits = Lists.newArrayList();
        List<DataSplit> nativeSplits = Lists.newArrayList();
        if (forceJni) {
            jniSplits = splits;
            return Pair.of(jniSplits, nativeSplits);
        }
        for (Split split : splits) {
            if (!(split instanceof DataSplit)) {
                jniSplits.add(split);
                continue;
            }
            DataSplit dataSplit = (DataSplit) split;
            if (!dataSplit.rawConvertible() || dataSplit.deletionFiles().isPresent()) {
                jniSplits.add(split);
                continue;
            }
            List<RawFile> rawFiles = dataSplit.convertToRawFiles().orElse(Collections.emptyList());
            // Currently only support orc/parquet format with s3 or hdfs storage
            if (!rawFiles.stream().allMatch(PaimonMetaClient::isNativeReaderSupported)) {
                jniSplits.add(split);
                continue;
            }
            nativeSplits.add(dataSplit);
        }
        return Pair.of(jniSplits, nativeSplits);
    }

    private static boolean isNativeReaderSupported(RawFile rawFile) {
        return isFileFormatSupported(rawFile) && isProtocolSupported(rawFile);
    }

    private static boolean isFileFormatSupported(RawFile rawFile) {
        return CoreOptions.FileFormatType.ORC.toString().equals(rawFile.format())
                || CoreOptions.FileFormatType.PARQUET.toString().equals(rawFile.format());
    }

    private static boolean isProtocolSupported(RawFile rawFile) {
        String[] tokens = rawFile.path().split("://");
        if (tokens.length != 2) {
            return false;
        }
        return supportedNativeProtocols.contains(tokens[0].toUpperCase());
    }

    private static String toCompatibleFormat(RawFile rawFile) {
        if (rawFile.format().equals(CoreOptions.FileFormatType.ORC.toString())) {
            return "ORC";
        } else if (rawFile.format().equals(CoreOptions.FileFormatType.PARQUET.toString())) {
            return "Parquet";
        }
        throw new UnsupportedOperationException(
                String.format("Unsupported format: %s", rawFile.format()));
    }

    private static String rawFileToJsonString(RawFile rawFile, DataFileMeta fileMeta) {
        Preconditions.checkNotNull(fileMeta);
        return String.format(
                "{\"path\":\"%s\",\"size\":%d,\"offset\":%d,\"length\":%d,\"format\":\"%s\",\"schemaId\":%d,\"rowCount\":%d,\"creationTimeEpochMillis\":%d}",
                rawFile.path(), fileMeta.fileSize(), rawFile.offset(), rawFile.length(),
                toCompatibleFormat(rawFile), rawFile.schemaId(), rawFile.rowCount(),
                fileMeta.creationTimeEpochMillis());
    }

    // This method is only for cpp's unit test
    public void initLocalFilesystem(Map<String, String> params) throws Exception {
        String dbName = params.get("database");
        String tableName = params.get("table");

        catalog.createDatabase(dbName, true);

        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.primaryKey("col_int", "col_str");
        schemaBuilder.partitionKeys("col_int");
        schemaBuilder.column("col_int", DataTypes.INT());
        schemaBuilder.column("col_str", DataTypes.STRING());
        Schema schema = schemaBuilder.build();

        Identifier jniTableId = Identifier.create(dbName, tableName);
        catalog.createTable(jniTableId, schema, true);
    }
}
