package org.byconity.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.byconity.common.aop.DefaultProxy;
import org.byconity.common.loader.ThreadContextClassLoader;
import org.byconity.common.metaclient.MetaClient;
import org.byconity.iceberg.params.IcebergParams;
import org.byconity.iceberg.util.IcebergRPNPredicateParser;
import org.byconity.iceberg.util.IcebergSchemaUtils;
import org.byconity.proto.IcebergMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

public class IcebergMetaClient implements MetaClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergMetaClient.class);
    private static final Gson GSON = new GsonBuilder().create();
    private static final Field FIELD_EQUALITY_IDS;

    static {
        try {
            Class<?> clazz = Class.forName("org.apache.iceberg.BaseFile");
            FIELD_EQUALITY_IDS = clazz.getDeclaredField("equalityIds");
            FIELD_EQUALITY_IDS.setAccessible(true);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    private final IcebergParams params;
    private final Catalog catalog;

    public IcebergMetaClient(IcebergParams params) {
        this.params = params;
        this.catalog = params.getCatalog();
    }

    public static IcebergMetaClient create(byte[] bytes) throws Throwable {
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(IcebergMetaClient.class.getClassLoader())) {
            IcebergParams params = IcebergParams.parseFrom(new String(bytes));
            return DefaultProxy.wrap(IcebergMetaClient.class, new Class[] {IcebergParams.class},
                    new Object[] {params});
        } catch (Throwable e) {
            LOGGER.error("Failed to create IcebergMetaClient, error={}", e.getMessage(), e);
            throw e;
        } finally {
            LOGGER.debug("IcebergMetaClient create params: {}", new String(bytes));
        }
    }

    public IcebergParams getParams() {
        return params;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    @Override
    public String[] listDatabases() {
        SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
        return nsCatalog.listNamespaces().stream().map(Namespace::toString).toArray(String[]::new);
    }

    @Override
    public String[] listTables(String database) {
        return catalog.listTables(Namespace.of(database)).stream().map(TableIdentifier::name)
                .toArray(String[]::new);
    }

    @Override
    public byte[] getIcebergSchema(Map<String, String> params) {
        String dbName = params.get("database");
        String tableName = params.get("table");
        Table table = catalog.loadTable(TableIdentifier.of(dbName, tableName));
        IcebergMeta.Schema proto =
                IcebergSchemaUtils.toProto(table.schema(), table.spec(), table.properties());
        return proto.toByteArray();
    }

    @Override
    public Map<String, Object> getIcebergScanInfo(Map<String, String> params) throws Exception {
        String dbName = params.get("database");
        String tableName = params.get("table");
        Set<String> requiredFields = Sets.newHashSet();
        if (params.containsKey("required_fields")) {
            requiredFields.addAll(Arrays.asList(params.get("required_fields").split(",")));
        }
        String rpnPredicate = params.get("rpn_predicate");

        Table table = catalog.loadTable(TableIdentifier.of(dbName, tableName));

        Schema projectedSchema = null;
        if (CollectionUtils.isNotEmpty(requiredFields)) {
            projectedSchema = new Schema(table.schema().columns().stream()
                    .filter(field -> requiredFields.contains(field.name()))
                    .collect(Collectors.toList()));
        }
        Expression filter = IcebergRPNPredicateParser.parse(table, rpnPredicate);

        List<byte[]> dataFiles = Lists.newArrayList();
        try (CloseableIterable<FileScanTask> fileScanTasks =
                table.newScan().filter(filter).project(projectedSchema).planFiles()) {
            for (FileScanTask planFile : fileScanTasks) {
                dataFiles.add(planFileToJsonBytes(table, planFile));
            }
        }

        Map<String, Object> res = Maps.newHashMap();
        res.put("data_files", dataFiles);
        return res;
    }

    private static byte[] planFileToJsonBytes(Table table, FileScanTask planFile) throws Exception {
        Preconditions.checkNotNull(planFile);
        DataFileDO dataFileDO = new DataFileDO(planFile.file());

        List<DeleteFile> deletes = planFile.deletes();
        if (CollectionUtils.isNotEmpty(deletes)) {
            for (DeleteFile delete : deletes) {
                dataFileDO.appendDeleteFile(table, delete);
            }
        }

        return GSON.toJson(dataFileDO).getBytes(Charset.defaultCharset());
    }

    @SuppressWarnings("all")
    private static class BaseFileDO {
        @SerializedName("path")
        protected final String path;
        @SerializedName("size")
        protected final long size;
        @SerializedName("format")
        protected final String format;
        @SerializedName("record_count")
        protected final long recordCount;

        private BaseFileDO(ContentFile<?> file) {
            path = file.path().toString();
            size = file.fileSizeInBytes();
            format = file.format().name();
            recordCount = file.recordCount();
        }
    }


    @SuppressWarnings("all")
    private static final class DataFileDO extends BaseFileDO {
        @SerializedName("position_delete_files")
        private final List<DeleteFileDO> positionDeleteFiles = Lists.newArrayList();
        @SerializedName("equality_delete_files")
        private final List<DeleteFileDO> equalityDeleteFiles = Lists.newArrayList();

        private DataFileDO(ContentFile<?> file) {
            super(file);
        }

        private void appendDeleteFile(Table table, DeleteFile deleteFile) throws Exception {
            switch (deleteFile.content()) {
                case POSITION_DELETES:
                    positionDeleteFiles.add(new DeleteFileDO(deleteFile, null));
                    break;
                case EQUALITY_DELETES: {
                    equalityDeleteFiles.add(new DeleteFileDO(deleteFile,
                            buildEqualityDeleteSchema(table, deleteFile)));
                    break;
                }
                default:
                    throw new UnsupportedOperationException(String
                            .format("Unsupported delete content type %s", deleteFile.content()));
            }
        }

        private String buildEqualityDeleteSchema(Table table, DeleteFile deleteFile)
                throws Exception {
            int[] equalityIds = (int[]) FIELD_EQUALITY_IDS.get(deleteFile);
            List<Types.NestedField> fields = Lists.newArrayList();
            for (int equalityId : equalityIds) {
                fields.add(table.schema().findField(equalityId));
            }
            return Base64.getEncoder().encodeToString(
                    IcebergSchemaUtils.toProto(new Schema(fields), null, null).toByteArray());
        }
    }


    @SuppressWarnings("all")
    private static final class DeleteFileDO extends BaseFileDO {
        @SerializedName("schema_base64")
        private final String schemaBase64;

        private DeleteFileDO(ContentFile<?> file, String schemaBase64) {
            super(file);
            this.schemaBase64 = schemaBase64;
        }
    }
}
