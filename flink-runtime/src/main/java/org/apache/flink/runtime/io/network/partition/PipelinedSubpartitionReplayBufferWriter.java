package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateSerializer;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateSerializerImpl;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author zhangzhengqi3
 * @date 2022/10/24
 * @time 11:57 下午
 */
public class PipelinedSubpartitionReplayBufferWriter {
    private final ChannelStateSerializer serializer;
    private final FSDataOutputStream bufferStream;
    private final DataOutputStream dataStream;
    private final AbstractChannelStateHandle.StateContentMetaInfo contentMetaInfo;

    public PipelinedSubpartitionReplayBufferWriter(
            FSDataOutputStream bufferStream,
            AbstractChannelStateHandle.StateContentMetaInfo contentMetaInfo) {
        this.serializer = new ChannelStateSerializerImpl();
        this.bufferStream = bufferStream;
        this.dataStream = new DataOutputStream(bufferStream);
        this.contentMetaInfo = contentMetaInfo;
        try {
            serializer.writeHeader(dataStream);
        } catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    public void writeReplayBuffer(Buffer buffer) {
        try {
            long offset = bufferStream.getPos();
            serializer.writeData(dataStream, buffer);
            long size = bufferStream.getPos() - offset;
            contentMetaInfo.withDataAdded(offset, size);
        } catch (IOException e) {
            throw new FlinkRuntimeException("can't save buffer for replay after downstream fail", e);
        }
    }

    public boolean isEmpty() {
        return dataStream.size() == 0;
    }

    public DataOutputStream getDataStream() {
        return dataStream;
    }

    public void close() throws IOException {
        dataStream.close();
    }
}
