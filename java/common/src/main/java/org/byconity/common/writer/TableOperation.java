package org.byconity.common.writer;

import org.byconity.proto.LakeTypes;

public interface TableOperation {

    void commit(byte[] commitMsgs); // byte is LakeTypes.LakeSinkCommitInfos

    void abort(byte[] commitMsgs); // byte is LakeTypes.LakeSinkCommitInfos
}


