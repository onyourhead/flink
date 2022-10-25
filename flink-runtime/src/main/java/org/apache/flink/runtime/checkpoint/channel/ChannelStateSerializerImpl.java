package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.io.network.buffer.Buffer;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.apache.flink.util.Preconditions;

import java.io.*;
import java.util.List;

import static java.lang.Math.addExact;

public class ChannelStateSerializerImpl implements ChannelStateSerializer {
    private static final int SERIALIZATION_VERSION = 0;

    @Override
    public void writeHeader(DataOutputStream dataStream) throws IOException {
        dataStream.writeInt(SERIALIZATION_VERSION);
    }

    @Override
    public void writeData(DataOutputStream stream, Buffer... flinkBuffers) throws IOException {
        stream.writeInt(getSize(flinkBuffers));
        for (Buffer buffer : flinkBuffers) {
            ByteBuf nettyByteBuf = buffer.asByteBuf();
            nettyByteBuf.getBytes(nettyByteBuf.readerIndex(), stream, nettyByteBuf.readableBytes());
        }
    }

    private int getSize(Buffer[] buffers) {
        int len = 0;
        for (Buffer buffer : buffers) {
            len = addExact(len, buffer.readableBytes());
        }
        return len;
    }

    @Override
    public void readHeader(InputStream stream) throws IOException {
        int version = readInt(stream);
        Preconditions.checkArgument(
                version == SERIALIZATION_VERSION, "unsupported version: " + version);
    }

    @Override
    public int readLength(InputStream stream) throws IOException {
        int len = readInt(stream);
        Preconditions.checkArgument(len >= 0, "negative state size");
        return len;
    }

    @Override
    public int readData(InputStream stream, ChannelStateByteBuffer buffer, int bytes)
            throws IOException {
        return buffer.writeBytes(stream, bytes);
    }

    private static int readInt(InputStream stream) throws IOException {
        return new DataInputStream(stream).readInt();
    }

    @Override
    public byte[] extractAndMerge(byte[] bytes, List<Long> offsets) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(out);
        byte[] merged = extractByOffsets(bytes, offsets);
        writeHeader(dataOutputStream);
        dataOutputStream.writeInt(merged.length);
        dataOutputStream.write(merged, 0, merged.length);
        dataOutputStream.close();
        return out.toByteArray();
    }

    private byte[] extractByOffsets(byte[] data, List<Long> offsets) throws IOException {
        DataInputStream lengthReadingStream =
                new DataInputStream(new ByteArrayInputStream(data, 0, data.length));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long prevOffset = 0;
        for (long offset : offsets) {
            lengthReadingStream.skipBytes((int) (offset - prevOffset));
            int dataWithLengthOffset = (int) offset + Integer.BYTES;
            out.write(data, dataWithLengthOffset, lengthReadingStream.readInt());
            prevOffset = dataWithLengthOffset;
        }
        return out.toByteArray();
    }

    @Override
    public long getHeaderLength() {
        return Integer.BYTES;
    }
}
