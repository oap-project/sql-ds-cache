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

package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Common API of APE reader wrappers.
 */
public abstract class ParquetRecordReaderWrapper extends RecordReader<Void, Object> {
  int batchSize = 0;
  int inputSplitRowGroupStartIndex = 0;
  int inputSplitRowGroupNum = 0;

  StructType sparkSchema;
  long[] bufferPtrs;
  long[] nullPtrs;

  boolean cacheEnabled = false;
  boolean preBufferEnabled = false;
  boolean redisEnabled = false;
  boolean remoteReaderCompressEnabled = false;
  String remoteReaderCompressCodec = "zstd";

  ColumnarBatch columnarBatch;

  ColumnVector[] columnVectors;
  public ParquetRecordReaderWrapper(int capacity) {
    this.batchSize = capacity;
  }

  public void getRequiredSplitRowGroup(ParquetInputSplit split, Configuration configuration)
          throws IOException {
    long splitStart = split.getStart();
    long splitSize = split.getLength();
    Path file = split.getPath();

    ParquetMetadata footer = readFooter(configuration, file);
    List<BlockMetaData> blocks = footer.getBlocks();

    Long currentOffset = 0L;
    Long PARQUET_MAGIC_NUMBER = 4L;
    currentOffset += PARQUET_MAGIC_NUMBER;

    int rowGroupIndex = 0;

    boolean flag = false;
    for (BlockMetaData blockMetaData : blocks) {
      // check mid point rather than start point to avoid data/task skew.
      if (splitStart <= currentOffset + blockMetaData.getCompressedSize() / 2 &&
              splitStart + splitSize >= currentOffset + blockMetaData.getCompressedSize() / 2) {
        if (!flag) {
          flag = true;
          inputSplitRowGroupStartIndex = rowGroupIndex;
        }
        inputSplitRowGroupNum ++;
      }
      rowGroupIndex ++;
      currentOffset += blockMetaData.getCompressedSize();
    }
  }

  public void setCacheEnabled(boolean cacheEnabled) {
    this.cacheEnabled = cacheEnabled;
  }

  public void setPreBufferEnabled(boolean preBufferEnabled) {
    this.preBufferEnabled = preBufferEnabled;
  }

  public void setRedisEnabled(boolean redisEnabled) {
    this.redisEnabled = redisEnabled;
  }

  public void setRemoteReaderCompressEnabled(boolean compressEnabled) {
    this.remoteReaderCompressEnabled = compressEnabled;
  }

  public void setRemoteReaderCompressCodec(String codec) {
    this.remoteReaderCompressCodec = codec;
  }
  public void setFilter(FilterPredicate predicate) {}

  public void setAgg(String aggExpresion) {}

  public void setPlasmaCacheRedis(String host, int port, String password) {}

  public void setPartitionInfo(StructType partitionColumns, InternalRow partitionValues) {}

  public boolean nextBatch() {
    return false;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (columnarBatch == null) initBatch();
    return nextBatch();
  }

  @Override
  public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public Object getCurrentValue() throws IOException, InterruptedException {
    return columnarBatch;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    // todo: impl getTotalRows method in JNI layer
    return 0;
  }

  abstract void initBatch();
}
