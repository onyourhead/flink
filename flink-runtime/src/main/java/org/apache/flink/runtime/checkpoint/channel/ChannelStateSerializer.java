/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static java.lang.Math.addExact;
import static java.lang.Math.min;

public interface ChannelStateSerializer {

    void writeHeader(DataOutputStream dataStream) throws IOException;

    void writeData(DataOutputStream stream, Buffer... flinkBuffers) throws IOException;

    void readHeader(InputStream stream) throws IOException;

    int readLength(InputStream stream) throws IOException;

    int readData(InputStream stream, ChannelStateByteBuffer buffer, int bytes) throws IOException;

    byte[] extractAndMerge(byte[] bytes, List<Long> offsets) throws IOException;

    long getHeaderLength();
}

