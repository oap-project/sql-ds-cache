/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet.vector.nativevector;

import org.apache.flink.formats.parquet.utils.Platform;
import org.apache.flink.table.data.vector.heap.HeapFloatVector;


public class NativeFloatVector extends HeapFloatVector implements NativeVector {

    private static final long serialVersionUID = 7216045902943789034L;

    private long bufferPtr = 0;
    private int typeSize = 0;
    private int capacity = 0;
    private long nullPtr = 0;

    public NativeFloatVector(int len, int typeLength) {
      super(len);
      typeSize = typeLength;
    }

    public void setPtr(long bufferPtr_, long nullPtr_, int size_) {
        bufferPtr = bufferPtr_;
        nullPtr = nullPtr_;
        capacity = size_;
    }

    @Override
    public long getBufferPtr() {
        return bufferPtr;
    }

    @Override
    public long getNullPtr() {
        return nullPtr;
    }

    @Override
    public boolean isNullAt(int i) {
        return !Platform.getBoolean(null, nullPtr + i);
    }

    @Override
    public float getFloat(int i){
      return Platform.getFloat(null, bufferPtr + i * typeSize);
    }
}
