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
import org.apache.flink.table.data.vector.writable.WritableIntVector;

public class NativeIntVector extends AbstractNativeVector implements WritableIntVector {

    public NativeIntVector(int len, int typeSize) {
        super(len, typeSize);
    }

    @Override
    public void setInt(int i, int i1) {
        // should not reach here in remote vectors
    }

    @Override
    public void setIntsFromBinary(int i, int i1, byte[] bytes, int i2) {
        // should not reach here in remote vectors
    }

    @Override
    public void setInts(int i, int i1, int i2) {
        // should not reach here in remote vectors
    }

    @Override
    public void setInts(int i, int i1, int[] ints, int i2) {
        // should not reach here in remote vectors
    }

    @Override
    public void fill(int i) {
        // should not reach here in remote vectors
    }

    @Override
    public int getInt(int i) {
        return Platform.getInt(null, bufferPtr + i * typeSize);
    }
}
