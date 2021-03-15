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

import java.io.IOException;
import java.util.List;

import com.intel.ape.ParquetReaderJNI;
import com.intel.ape.util.ParquetFilterPredicateConvertor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;

import org.apache.spark.sql.execution.vectorized.NativeColumnVector;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.Platform;

public class ParquetNativeRecordReaderWrapper extends RecordReader<Void, Object> {
  private static final Logger LOG =
          LoggerFactory.getLogger(ParquetNativeRecordReaderWrapper.class);

  long reader = 0;
  int batchSize = 0;

  long[] bufferPtrs;
  long[] nullPtrs;

  long readTime = 0;

  boolean cacheEnabled = false;

  private ColumnarBatch columnarBatch;

  private NativeColumnVector[] columnVectors;

  private int inputSplitRowGroupStartIndex = 0;
  private int inputSplitRowGroupNum = 0;

  StructType sparkSchema;

  public ParquetNativeRecordReaderWrapper(int capacity) {
    this.batchSize = capacity;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException {
    Configuration configuration = taskAttemptContext.getConfiguration();
    String reqSchema = configuration.get(ParquetReadSupport$.MODULE$.SPARK_ROW_REQUESTED_SCHEMA());
    sparkSchema = StructType$.MODULE$.fromString(reqSchema);
    ParquetInputSplit split = (ParquetInputSplit) inputSplit;

    getRequiredSplitRowGroup(split, configuration);

    String fileName = split.getPath().toUri().getRawPath();
    String hdfs = configuration.get("fs.defaultFS"); // this string is like hdfs://host:port
    String[] res = hdfs.split(":");
    String hdfsHost = res[1].substring(2);
    int hdfsPort = Integer.parseInt(res[2]);
    LOG.info("filename is " + fileName + " hdfs is " + hdfsHost + " " + hdfsPort);
    LOG.info("schema is " + sparkSchema.json());
    reader = ParquetReaderJNI.init(fileName, hdfsHost, hdfsPort, sparkSchema.json(),
            inputSplitRowGroupStartIndex, inputSplitRowGroupNum, cacheEnabled);
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
      if (splitStart <= currentOffset && splitStart + splitSize >= currentOffset) {
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

  public void setFilter(FilterPredicate predicate) {
    String predicateStr = ParquetFilterPredicateConvertor.toJsonString(predicate);
    ParquetReaderJNI.setFilterStr(reader, predicateStr);
  }

  public void setAgg(String aggExpresion) {
    ParquetReaderJNI.setAggStr(reader, aggExpresion);
  }

  public void setPlasmaCacheRedis(String host, int port, String password) {
    ParquetReaderJNI.setPlasmaCacheRedis(reader, host, port, password);
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

  @Override
  public void close() throws IOException {
    // close columnBatch
    if (columnarBatch != null) {
      columnarBatch.close();
      columnarBatch = null;
    }

    // close reader
    if (reader != 0) {
      ParquetReaderJNI.close(reader);
    }

    // free buffers
    for (int i = 0; i < columnVectors.length; i++) {
      Platform.freeMemory(nullPtrs[i]);
      Platform.freeMemory(bufferPtrs[i]);
    }

    LOG.info("close reader, spend time: " + readTime + " ns");
  }

  private boolean nextBatch() {
    long before = System.nanoTime();
    if (reader == 0) {
      return false;
    }
    int rowsRead = 0;
    while (rowsRead == 0) {
      rowsRead = ParquetReaderJNI.readBatch(reader, batchSize, bufferPtrs, nullPtrs);
    }

    if (rowsRead < 0) {
      return false;
    }
    // build columnVectors by buffers and nulls
    columnarBatch.setNumRows(0);

    columnarBatch.setNumRows(rowsRead);
    readTime += (System.nanoTime() - before);
    return true;
  }

  private void initBatch() {
    // allocate buffers here according schema
    StructField[] fileds = sparkSchema.fields();
    columnVectors = new NativeColumnVector[fileds.length];
    bufferPtrs = new long[fileds.length];
    nullPtrs = new long[fileds.length];
    for (int i = 0; i < fileds.length; i++) {
      DataType type = fileds[i].dataType();
      columnVectors[i] = new NativeColumnVector(type);

      long nullPtr = Platform.allocateMemory(batchSize);
      long bufferPtr = 0;
      if (type instanceof BooleanType || type instanceof IntegerType || type instanceof LongType
              || type instanceof FloatType || type instanceof DoubleType
              || type instanceof DateType || type instanceof TimestampType) {
        bufferPtr = Platform.allocateMemory(batchSize * type.defaultSize());
      } else if (type instanceof BinaryType || type instanceof StringType) {
        bufferPtr = Platform.allocateMemory(batchSize * 16);
      } else if (DecimalType.is32BitDecimalType(type)) {
        bufferPtr = Platform.allocateMemory(batchSize * 4);
      } else if (DecimalType.is64BitDecimalType(type)) {
        bufferPtr = Platform.allocateMemory(batchSize * 8);
      } else {
        // TODO: will add byte and short type. Not sure about Map
        throw new UnsupportedOperationException("Type not support yet");
      }
      columnVectors[i].set(bufferPtr, nullPtr, batchSize);
      bufferPtrs[i] = bufferPtr;
      nullPtrs[i] = nullPtr;
    }

    columnarBatch = new ColumnarBatch(columnVectors);
  }
}
