package org.byconity.paimon;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.FileUtils;
import org.byconity.paimon.params.PaimonFilesystemParams;
import org.byconity.paimon.params.PaimonParams;

import java.io.File;
import java.util.Map;

public class PaimonTestBase {
    protected static final Gson GSON = new GsonBuilder().create();

    protected static Map<String, String> createLocalParams() throws Exception {
        File directory = new File("/tmp/paimon_test");
        if (directory.exists()) {
            FileUtils.deleteDirectory(directory);
        }
        Map<String, String> params = Maps.newHashMap();
        params.put("metastoreType", PaimonParams.MetastoreType.filesystem.name());
        params.put("filesystemType", PaimonFilesystemParams.FilesystemType.LOCAL.name());
        params.put("path", directory.getAbsolutePath());
        return params;
    }
}
