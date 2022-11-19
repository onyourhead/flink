package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateSerializer;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateSerializerImpl;
import org.apache.flink.runtime.checkpoint.channel.RecoveredChannelStateHandler;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;

import java.io.IOException;

import static org.apache.flink.runtime.checkpoint.channel.ChannelStateByteBuffer.wrap;

/**
 * @author zhangzhengqi3
 * @date 2022/10/25
 * @time 12:18 上午
 */
public class PipelinedSubpartitionBufferReplayer {

    private final ChannelStateSerializer serializer;
    private final MemCheckpointStreamFactory.MemoryCheckpointOutputStream outputStream;
    private final AbstractChannelStateHandle.StateContentMetaInfo contentMetaInfo;


    public PipelinedSubpartitionBufferReplayer(MemCheckpointStreamFactory.MemoryCheckpointOutputStream outputStream,
                                               AbstractChannelStateHandle.StateContentMetaInfo contentMetaInfo) {
        this.outputStream = outputStream;
        this.serializer = new ChannelStateSerializerImpl();
        this.contentMetaInfo = contentMetaInfo;
    }

     void readAndRecoverBuffer(PipelinedSubpartition pipelinedSubpartition) throws IOException, InterruptedException {
         byte[] bytes = outputStream.flushAndGetBytes();
         byte[] merge = serializer.extractAndMerge(bytes, contentMetaInfo.getOffsets());
         try (FSDataInputStream source = new ByteStreamStateHandle.ByteStateHandleInputStream(
                 merge)) {
             long headerLength = serializer.getHeaderLength();
             if (source.getPos() != headerLength) {
                 source.seek(headerLength);
             }
             int length = serializer.readLength(source);
             while (length > 0) {
                 RecoveredChannelStateHandler.BufferWithContext<BufferBuilder> bufferWithContext =
                         getBuffer(pipelinedSubpartition);
                 while (length > 0 && bufferWithContext.buffer.isWritable()) {
                     length -= serializer.readData(source, bufferWithContext.buffer, length);
                 }
                 // Passing the ownership of buffer to inside.
                 recover(bufferWithContext, pipelinedSubpartition);
             }
         }
    }

    public void recover(
            RecoveredChannelStateHandler.BufferWithContext<BufferBuilder> bufferWithContext,
            PipelinedSubpartition pipelinedSubpartition) throws IOException {
        try (BufferBuilder bufferBuilder = bufferWithContext.context) {
            try (BufferConsumer bufferConsumer =
                         bufferBuilder.createBufferConsumerFromBeginning()) {
                bufferBuilder.finish();
                if (bufferConsumer.isDataAvailable()) {
                    pipelinedSubpartition.addReplayBufferConsumer(bufferConsumer.copy());
                }
            }
        }
    }

    public RecoveredChannelStateHandler.BufferWithContext<BufferBuilder> getBuffer(PipelinedSubpartition pipelinedSubpartition)
            throws IOException, InterruptedException {
        BufferBuilder bufferBuilder = pipelinedSubpartition.requestBufferBuilderBlocking();
        return new RecoveredChannelStateHandler.BufferWithContext<>(wrap(bufferBuilder), bufferBuilder);
    }

    public void close() {
        outputStream.close();
    }
}
