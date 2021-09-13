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

import org.apache.flink.table.data.vector.writable.WritableBytesVector;

/** Vector implementation. */
public class RemoteFixedBytesVector extends AbstractRemoteVector implements WritableBytesVector {
    public RemoteFixedBytesVector(int vectorLength, int typeSize) {
        super(vectorLength, typeSize);
    }

    @Override
    public void appendBytes(int i, byte[] bytes, int i1, int i2) {
        // should not reach here in remote vectors
    }

    @Override
    public void fill(byte[] bytes) {
        // should not reach here in remote vectors
    }

    @Override
    public Bytes getBytes(int i) {
        byte[] str = new byte[typeSize];
        dataBuf.getBytes(i * typeSize, str);
        return new Bytes(str, 0, str.length);
    }
}
