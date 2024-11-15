package org.byconity.las;

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.byconity.proto.HudiMeta;
import org.eclipse.core.internal.dtree.ObjectNotFoundException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Modified from org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopInputSplit. Skip the
 * serialization of JobConf. Therefore, need to invoke {@link #initInputSplit(JobConf)} after
 * deserialization.
 */
public class OptimizedHadoopInputSplit implements Serializable {
    private static final long serialVersionUID = -6990336376163226161L;
    private final Class<? extends InputSplit> splitType;
    private transient InputSplit hadoopInputSplit;
    private byte[] hadoopInputSplitByteArray;
    int partitionIndex;

    public OptimizedHadoopInputSplit(InputSplit hInputSplit) {
        if (hInputSplit == null) {
            throw new NullPointerException("Hadoop input split must not be null");
        }

        this.splitType = hInputSplit.getClass();
        this.hadoopInputSplit = hInputSplit;
    }

    public OptimizedHadoopInputSplit(InputSplit hInputSplit, int partitionIndex) {
        this(hInputSplit);
        this.partitionIndex = partitionIndex;
    }

    public InputSplit getHadoopInputSplit() {
        return this.hadoopInputSplit;
    }

    /**
     * Initialize inputSplit with jobConf after deserialization
     */
    public void initInputSplit(JobConf jobConf) {
        if (this.hadoopInputSplit != null) {
            return;
        }

        if (null == this.hadoopInputSplitByteArray) {
            throw new NullPointerException("hadoopInputSplitByteArray must not be null");
        }

        try {
            this.hadoopInputSplit = (InputSplit) WritableFactories.newInstance(splitType);

            if (this.hadoopInputSplit instanceof Configurable) {
                ((Configurable) this.hadoopInputSplit).setConf(jobConf);
            } else if (this.hadoopInputSplit instanceof JobConfigurable) {
                ((JobConfigurable) this.hadoopInputSplit).configure(jobConf);
            }

            if (hadoopInputSplitByteArray != null) {
                try (ObjectInputStream oInput = new ObjectInputStream(
                        new ByteArrayInputStream(hadoopInputSplitByteArray))) {
                    this.hadoopInputSplit.readFields(oInput);
                }

                this.hadoopInputSplitByteArray = null;
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to instantiate Hadoop InputSplit", e);
        }
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        if (hadoopInputSplit != null) {
            try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
                    ObjectOutputStream oout = new ObjectOutputStream(bout)) {
                this.hadoopInputSplit.write(oout);
                oout.flush();
                this.hadoopInputSplitByteArray = bout.toByteArray();
            }
        }

        out.defaultWriteObject();
    }

    private byte[] serialized;

    public void prepareSerializedObject() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {

            oos.writeObject(this);
            oos.flush();
            serialized = bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    byte[] getSerializedObject() {
        if (serialized == null)
            throw new ObjectNotFoundException("Prepared serialized object not found");
        return serialized;
    }

    public HudiMeta.InputSplit serializeProtoBuf() {
        HudiMeta.InputSplit.Builder proto = HudiMeta.InputSplit.newBuilder();
        proto.setInputSplitBytes(ByteString.copyFrom(getSerializedObject()));
        proto.setPartitionIndex(partitionIndex);
        return proto.build();
    }
}
