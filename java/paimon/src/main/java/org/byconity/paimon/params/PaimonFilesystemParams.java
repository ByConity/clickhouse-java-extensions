package org.byconity.paimon.params;

import com.google.common.base.Preconditions;
import com.google.gson.JsonObject;

public abstract class PaimonFilesystemParams extends PaimonParams {

    private static final String FILESYSTEM_TYPE = "filesystemType";

    public static PaimonFilesystemParams parseFrom(JsonObject json) {
        Preconditions.checkState(json.has(FILESYSTEM_TYPE), "missing field " + FILESYSTEM_TYPE);
        FilesystemType filesystemType =
                FilesystemType.valueOf(json.get(FILESYSTEM_TYPE).getAsString());
        switch (filesystemType) {
            case LOCAL:
                return PaimonLocalFilesystemParams.parseFrom(json);
            case HDFS:
                return PaimonHDFSParams.parseFrom(json);
            case S3:
                return PaimonS3Params.parseFrom(json);
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported filesystemType: %s", filesystemType.name()));
        }
    }

    @Override
    public final MetastoreType getMetastoreType() {
        return MetastoreType.filesystem;
    }

    public abstract FilesystemType getFilesystemType();

    public enum FilesystemType {
        LOCAL, HDFS, S3, OSS
    }
}
