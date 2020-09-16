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

package org.apache.spark.sql.execution.datasources.parquet;

import org.apache.spark.sql.execution.cacheUtil.FiberCache;
import org.apache.spark.sql.execution.cacheUtil.OapFiberCache;
import org.apache.spark.sql.execution.vectorized.ReadOnlyColumnVectorV1;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;

public class VectorizedCacheReader {

  FiberCache fiberCache;
  DataType type;

  int index;
  int total;
  int typeSize;

  // TODO: pass some parameters or via init method.
  public VectorizedCacheReader(DataType type, FiberCache fiberCache) {
    this.index = 0;
    this.fiberCache = fiberCache;
    this.type = type;
    this.typeSize = type.defaultSize();
  }

  public void init() {
    return ;
  }

  public ColumnVector readBatch(int num) {
    if(fiberCache instanceof OapFiberCache) {
      // TODO: How to get offset/ address
      long addr = ((OapFiberCache) fiberCache).getBuffer().address();
      long header = 6; // total: Int, nonull: Boolean, allNull: Boolean
      long nullOffset = addr + header + index;
      long dataOffset = addr + header + total + index * typeSize;
      ColumnVector column = new ReadOnlyColumnVectorV1(type, nullOffset, dataOffset, num);
      index += num;
      return column;
    } else {
      throw new UnsupportedOperationException("Only support OAPFiberCache Now");
    }
  }

}
