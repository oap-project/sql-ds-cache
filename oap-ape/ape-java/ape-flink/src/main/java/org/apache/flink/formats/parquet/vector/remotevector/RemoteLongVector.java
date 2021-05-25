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

import org.apache.flink.table.data.vector.writable.WritableLongVector;

public class RemoteLongVector extends AbstractRemoteVector implements WritableLongVector {
    public RemoteLongVector(int vectorLength, int typeSize) {
        super(vectorLength, typeSize);
    }

    @Override
    public void setLong(int i, long l) {
        // should not reach here in remote vectors
    }

    @Override
    public void setLongsFromBinary(int i, int i1, byte[] bytes, int i2) {
        // should not reach here in remote vectors
    }

    @Override
    public void fill(long l) {
        // should not reach here in remote vectors
    }

    @Override
    public long getLong(int i) {
        return dataBuf.getLongLE(i * typeSize);
    }
}