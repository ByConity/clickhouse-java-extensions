package org.byconity.iceberg.params;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.net.URI;
import java.util.Collections;
import java.util.Set;

public abstract class IcebergHadoopParams extends IcebergParams {
    private static final Set<String> PROTOCOL_S3_FAMILY = Collections
            .unmodifiableSet(Sets.newHashSet("S3", "S3A", "COSN", "COS", "OBS", "OSS", "TOS"));

    @SerializedName("warehouse")
    private String warehouse;

    public static IcebergHadoopParams parseFrom(JsonObject json) {
        Preconditions.checkState(json.has(CatalogProperties.WAREHOUSE_LOCATION),
                "warehouse is required for hadoop metastore");
        URI uri = URI.create(json.get(CatalogProperties.WAREHOUSE_LOCATION).getAsString());
        if (StringUtils.equalsIgnoreCase("HDFS", uri.getScheme())) {
            return IcebergHDFSParams.parseFrom(json);
        } else if (StringUtils.equalsIgnoreCase("FILE", uri.getScheme())) {
            return IcebergLocalParams.parseFrom(json);
        } else if (PROTOCOL_S3_FAMILY.contains(uri.getScheme().toUpperCase())) {
            return IcebergS3Params.parseFrom(json);
        }
        throw new UnsupportedOperationException(
                String.format("Unsupported protocol: %s", uri.getScheme()));
    }

    @Override
    public final MetastoreType getMetastoreType() {
        return MetastoreType.hadoop;
    }

    protected void fillOptions(Configuration hadoopConf) {

    }

    @Override
    public final Catalog getCatalog() {
        Configuration hadoopConf = new Configuration();
        Preconditions.checkState(StringUtils.isNotBlank(warehouse),
                "warehouse is required by HadoopCatalog");
        URI uri = URI.create(warehouse);
        if (uri.getPort() > 0) {
            hadoopConf.set("fs.defaultFS",
                    String.format("%s://%s:%d", uri.getScheme(), uri.getHost(), uri.getPort()));
        } else {
            hadoopConf.set("fs.defaultFS",
                    String.format("%s://%s", uri.getScheme(), uri.getHost()));
        }
        fillOptions(hadoopConf);
        return new HadoopCatalog(hadoopConf, warehouse);
    }

    public static void main(String[] args) {
        URI uri = URI.create("hdfs://hadoop/user/path");
        System.out.println(uri.getHost());
        System.out.println(uri.getPort());
    }

    public abstract FilesystemType getFilesystemType();

    public enum FilesystemType {
        LOCAL, HDFS, S3
    }
}
