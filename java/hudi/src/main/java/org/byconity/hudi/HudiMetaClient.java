package org.byconity.hudi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.byconity.common.aop.DefaultProxy;
import org.byconity.common.loader.ThreadContextClassLoader;
import org.byconity.common.metaclient.MetaClient;
import org.byconity.proto.HudiMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class HudiMetaClient implements MetaClient {
    protected static final Logger LOGGER = LoggerFactory.getLogger(HudiMetaClient.class);

    private final ClassLoader classLoader = getClass().getClassLoader();
    private final String basePath;
    private final HadoopStorageConfiguration configuration;

    public static HudiMetaClient create(byte[] raw) throws Exception {
        Map<String, String> params = null;
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(HudiMetaClient.class.getClassLoader())) {
            HudiMeta.HudiMetaClientParams pb_params = HudiMeta.HudiMetaClientParams.parseFrom(raw);
            params = pb_params.getProperties().getPropertiesList().stream()
                    .collect(Collectors.toMap(HudiMeta.Properties.KeyValue::getKey,
                            HudiMeta.Properties.KeyValue::getValue));
            String basePath = params.get("base_path");
            Configuration config = new Configuration();
            for (Map.Entry<String, String> kv : params.entrySet()) {
                config.set(kv.getKey(), kv.getValue());
            }
            String hdfsSitePath = System.getenv("hdfs_site_path");
            if (hdfsSitePath != null) {
                config.addResource(new Path(hdfsSitePath));
                LOGGER.info("load hdfs-site.xml from {}", hdfsSitePath);
            }
            String coreSitePath = System.getenv("core_site_path");
            if (coreSitePath != null) {
                config.addResource(new Path(coreSitePath));
                LOGGER.info("load core-site.xml from {}", coreSitePath);
            }

            return DefaultProxy.wrap(HudiMetaClient.class,
                    new Class[] {String.class, Configuration.class},
                    new Object[] {basePath, config});
        } catch (Throwable e) {
            LOGGER.error("Failed to create HudiMetaClient, error={}", e.getMessage(), e);
            throw e;
        } finally {
            LOGGER.debug("HudiMetaClient create params: {}", params);
        }
    }

    public HudiMetaClient(String base_path, Configuration conf) {
        this.basePath = base_path;
        this.configuration = new HadoopStorageConfiguration(conf);
    }

    @Override
    public byte[] getFilesInPartition(byte[] params) throws Exception {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            HudiMeta.PartitionPaths partitionPaths = HudiMeta.PartitionPaths.parseFrom(params);

            String keytab = System.getenv("krb_keytab");
            if (keytab != null) {
                String user = System.getenv("krb_principle");
                UserGroupInformation.loginUserFromKeytab(user, keytab);
                LOGGER.info("Kerberos current user: {}", UserGroupInformation.getCurrentUser());
                LOGGER.info("Kerberos login user: {}", UserGroupInformation.getLoginUser());
            }

            HudiMeta.HudiFileSlices.Builder builder = HudiMeta.HudiFileSlices.newBuilder();

            HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                    .setConf(configuration).setBasePath(basePath).build();
            metaClient.reloadActiveTimeline();
            HoodieTimeline timeline =
                    metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
            Option<HoodieInstant> latestInstant = timeline.lastInstant();
            String queryInstant = latestInstant.get().getTimestamp();
            builder.setInstant(queryInstant);

            int partition_idx = 0;
            for (String partitionPath : partitionPaths.getPathsList()) {
                String partitionName = FSUtils.getRelativePartitionPath(new StoragePath(basePath),
                        new StoragePath(partitionPath));
                String globPath = String.format("%s/%s/*", basePath, partitionName);
                List<StoragePathInfo> statuses = FSUtils.getGlobStatusExcludingMetaFolder(
                        metaClient.getStorage(), new StoragePath(globPath));
                HoodieTableFileSystemView fileSystemView =
                        new HoodieTableFileSystemView(metaClient, timeline, statuses);
                Iterator<FileSlice> hoodieFileSliceIterator = fileSystemView
                        .getLatestMergedFileSlicesBeforeOrOn(partitionName, queryInstant)
                        .iterator();
                while (hoodieFileSliceIterator.hasNext()) {
                    FileSlice fileSlice = hoodieFileSliceIterator.next();
                    Optional<HoodieBaseFile> baseFile = fileSlice.getBaseFile().toJavaOptional();
                    String fileName = baseFile.map(HoodieBaseFile::getFileName).orElse("");
                    long fileLength = baseFile.map(HoodieBaseFile::getFileLen).orElse(-1L);

                    List<String> logs = fileSlice.getLogFiles().map(HoodieLogFile::getFileName)
                            .collect(Collectors.toList());
                    builder.addFileSlices(HudiMeta.HudiFileSlices.FileSlice.newBuilder()
                            .setBaseFileName(fileName).setBaseFileLength(fileLength)
                            .addAllDeltaLogs(logs).setPartitionIndex(partition_idx).build());
                }

                partition_idx++;
            }

            return builder.build().toByteArray();
        }
    }
}
