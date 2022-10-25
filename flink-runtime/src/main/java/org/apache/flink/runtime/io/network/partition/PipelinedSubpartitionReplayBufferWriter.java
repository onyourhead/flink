package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.checkpoint.channel.ChannelStateSerializer;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateSerializerImpl;
import org.apache.flink.runtime.io.network.buffer.Buffer;
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
    private final DataOutputStream dataStream;

    public PipelinedSubpartitionReplayBufferWriter(
            DataOutputStream dataStream) {
        this.serializer = new ChannelStateSerializerImpl();
        this.dataStream = dataStream;
    }

    public void writeReplayBuffer(Buffer buffer) {
        try {
            serializer.writeData(dataStream, buffer);
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
}
