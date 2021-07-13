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

package org.apache.flink.formats.parquet.vector.nativevector;

import org.apache.flink.formats.parquet.utils.Platform;
import org.apache.flink.table.data.vector.writable.AbstractWritableVector;
import org.apache.flink.table.data.vector.writable.WritableIntVector;

public abstract class AbstractNativeVector extends AbstractWritableVector {
    protected final int vectorLength;
    protected final int typeSize;

    protected long bufferPtr = 0;
    protected long nullPtr = 0;

    public AbstractNativeVector(int vectorLength, int typeSize) {
        this.vectorLength = vectorLength;
        this.typeSize = typeSize;
    }

    public void setPtr(long bufferPtr_, long nullPtr_, int size_) {
        bufferPtr = bufferPtr_;
        nullPtr = nullPtr_;
    }

    public long getBufferPtr() {
        return bufferPtr;
    }

    public long getNullPtr() {
        return nullPtr;
    }

    @Override
    public boolean isNullAt(int i) {
        return !Platform.getBoolean(null, nullPtr + i);
    }

    @Override
    public void setNullAt(int i) {
        Platform.putBoolean(null, nullPtr + i, false);
    }

    @Override
    public void setNulls(int i, int i1) {
        for (int j = 0; j < i1; j++) {
            setNullAt(j);
        }
    }

    @Override
    public void fillWithNulls() {
        for (int i = 0; i < vectorLength; i++) {
            setNullAt(i);
        }
    }

    @Override
    public void reset() {

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
