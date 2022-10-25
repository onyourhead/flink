package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.io.InputStream;

import static java.lang.Math.min;

/** Wrapper around various buffers to receive channel state data. */
@Internal
@NotThreadSafe
public interface ChannelStateByteBuffer extends AutoCloseable {

    boolean isWritable();

    @Override
    void close();

    /**
     * Read up to <code>bytesToRead</code> bytes into this buffer from the given {@link
     * InputStream}.
     *
     * @return the total number of bytes read into this buffer.
     */
    int writeBytes(InputStream input, int bytesToRead) throws IOException;

    static ChannelStateByteBuffer wrap(Buffer buffer) {
        return new ChannelStateByteBuffer() {

            private final ByteBuf byteBuf = buffer.asByteBuf();

            @Override
            public boolean isWritable() {
                return byteBuf.isWritable();
            }

            @Override
            public void close() {
                buffer.recycleBuffer();
            }

            @Override
            public int writeBytes(InputStream input, int bytesToRead) throws IOException {
                return byteBuf.writeBytes(input, Math.min(bytesToRead, byteBuf.writableBytes()));
            }
        };
    }

    static ChannelStateByteBuffer wrap(BufferBuilder bufferBuilder) {
        final byte[] buf = new byte[1024];
        return new ChannelStateByteBuffer() {
            @Override
            public boolean isWritable() {
                return !bufferBuilder.isFull();
            }

            @Override
            public void close() {
                bufferBuilder.close();
            }

            @Override
            public int writeBytes(InputStream input, int bytesToRead) throws IOException {
                int left = bytesToRead;
                for (int toRead = getToRead(left); toRead > 0; toRead = getToRead(left)) {
                    int read = input.read(buf, 0, toRead);
                    int copied = bufferBuilder.append(java.nio.ByteBuffer.wrap(buf, 0, read));
                    Preconditions.checkState(copied == read);
                    left -= read;
                }
                bufferBuilder.commit();
                return bytesToRead - left;
            }

            private int getToRead(int bytesToRead) {
                return min(bytesToRead, min(buf.length, bufferBuilder.getWritableBytes()));
            }
        };
    }

    static ChannelStateByteBuffer wrap(byte[] bytes) {
        return new ChannelStateByteBuffer() {
            private int written = 0;

            @Override
            public boolean isWritable() {
                return written < bytes.length;
            }

            @Override
            public void close() {
            }

            @Override
            public int writeBytes(InputStream input, int bytesToRead) throws IOException {
                final int bytesRead = input.read(bytes, written, bytes.length - written);
                written += bytesRead;
                return bytesRead;
            }
        };
    }
}
