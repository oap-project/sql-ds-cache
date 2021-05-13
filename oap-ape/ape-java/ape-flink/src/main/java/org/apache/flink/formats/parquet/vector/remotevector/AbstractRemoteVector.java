/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet.vector.remotevector;

import io.netty.buffer.ByteBuf;

import org.apache.flink.table.data.vector.writable.AbstractWritableVector;
import org.apache.flink.table.data.vector.writable.WritableIntVector;

public abstract class AbstractRemoteVector extends AbstractWritableVector {
    protected final int vectorLength;
    protected final int typeSize;

    private int trackingId = 0; // tracking batch ID for resource recycling.

    protected ByteBuf dataBuf;
    protected ByteBuf nullBuf;

    protected ByteBuf elementLengthBuf;

    public AbstractRemoteVector(int vectorLength, int typeSize) {
        this.vectorLength = vectorLength;
        this.typeSize = typeSize;
    }

    public void setBuffers(ByteBuf dataBuf, ByteBuf nullBuf, ByteBuf elementLengthBuf) {
        this.dataBuf = dataBuf;
        this.nullBuf = nullBuf;
        this.elementLengthBuf = elementLengthBuf;
    }

    public void setTrackingId(int id) {
        trackingId = id;
    }

    public int getTrackingId() {
        return trackingId;
    }

    @Override
    public void reset() {
        // Only element length buffer is sliced from batch response.
        // Data buffer and null buffer will be released by response itself.
        if (elementLengthBuf != null) {
            elementLengthBuf.release();
            elementLengthBuf = null;
        }
    }

    @Override
    public boolean isNullAt(int i) {
        return !(nullBuf.getBoolean(i));
    }

    @Override
    public void setNullAt(int i) {
        nullBuf.setBoolean(i, false);
    }

    @Override
    public void setNulls(int i, int i1) {
        for (int j = 0; j < i1; j++) {
            setNullAt(j);
        }
    }

    @Override
    public void fillWithNulls() {
        for (int i = 0; i < nullBuf.readableBytes(); i++) {
            setNullAt(i);
        }
    }

    @Override
    public WritableIntVector reserveDictionaryIds(int i) {
        return null;
    }

    @Override
    public WritableIntVector getDictionaryIds() {
        return null;
    }
}
