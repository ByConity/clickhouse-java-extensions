package org.byconity.hudi.reader;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.byconity.common.loader.ThreadContextClassLoader;
import org.byconity.proto.HudiMeta;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HudiArrowReaderBuilder extends InputFormatArrowReaderBuilder {

    private final String basePath;
    private final String instantTime;
    private final String dataFilePath;
    private final long dataFileLength;
    private final String[] deltaFilePaths;

    public static HudiArrowReaderBuilder create(byte[] raw) throws InvalidProtocolBufferException {
        Map<String, String> params = null;
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(HudiArrowReaderBuilder.class.getClassLoader())) {
            BufferAllocator allocator = new RootAllocator();
            HudiMeta.Properties properties = HudiMeta.Properties.parseFrom(raw);
            params = properties.getPropertiesList().stream().collect(Collectors.toMap(
                    HudiMeta.Properties.KeyValue::getKey, HudiMeta.Properties.KeyValue::getValue));

            return new HudiArrowReaderBuilder(allocator, params);
        } catch (Throwable e) {
            LOGGER.error("Failed to create HudiArrowReaderBuilder, error={}", e.getMessage(), e);
            throw e;
        } finally {
            LOGGER.debug("HudiArrowReaderBuilder create params: {}", params);
        }
    }

    public HudiArrowReaderBuilder(BufferAllocator allocator, Map<String, String> params) {
        super(allocator, params);
        this.basePath = getOrThrow(params, "base_path");
        this.instantTime = getOrThrow(params, "instant_time");
        this.dataFilePath = params.getOrDefault("data_file_path", "");
        this.dataFileLength = Long.parseLong(params.getOrDefault("data_file_length", "-1"));
        this.deltaFilePaths = params.getOrDefault("delta_file_paths", "").split(",");
    }

    @Override
    protected Configuration makeConfiguration() throws IOException {
        Configuration configuration = new Configuration();

        String keytab = System.getenv("krb_keytab");
        if (keytab != null) {
            String user = System.getenv("krb_principle");
            UserGroupInformation.loginUserFromKeytab(user, keytab);
            System.out.println("Kerberos current user: " + UserGroupInformation.getCurrentUser());
            System.out.println("Kerberos login user: " + UserGroupInformation.getLoginUser());
        }

        return configuration;
    }

    @Override
    public InputSplit createInputSplit(JobConf jobConf) throws IOException {
        // dataFileLenth==-1 or dataFilePath == "" means logs only scan
        String realtimePath = dataFileLength != -1 ? dataFilePath : deltaFilePaths[0];
        long realtimeLength = dataFileLength != -1 ? dataFileLength : 0;

        Path path = new Path(realtimePath);
        FileSplit fileSplit = new FileSplit(path, 0, realtimeLength, (String[]) null);
        List<HoodieLogFile> logFiles =
                Arrays.stream(deltaFilePaths).filter(x -> x != null && !x.isEmpty())
                        .map(HoodieLogFile::new).collect(Collectors.toList());
        return new HoodieRealtimeFileSplit(fileSplit, basePath, logFiles, instantTime, false,
                Option.empty());
    }
}
