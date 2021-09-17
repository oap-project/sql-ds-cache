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

import com.intel.ape.ParquetReaderJNI;
import com.intel.ape.util.ParquetFilterPredicateConvertor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.NativeColumnVector;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.Platform;

public class ParquetNativeRecordReaderWrapper extends ParquetRecordReaderWrapper {
  private static final Logger LOG =
          LoggerFactory.getLogger(ParquetNativeRecordReaderWrapper.class);

  long reader = 0;
  long readTime = 0;

  private FilterPredicate filterPredicate;
  private String aggExpr;

  private StructType partitionColumns = null;
  private InternalRow partitionValues = null;

  public ParquetNativeRecordReaderWrapper(int capacity) {
    super(capacity);
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
            inputSplitRowGroupStartIndex, inputSplitRowGroupNum, cacheEnabled, preBufferEnabled,
            false);
    if (filterPredicate != null) {
      setFilterToNative(filterPredicate);
    }
    if (aggExpr != null) {
      setAggToNative(aggExpr);
    }
  }

  private void setFilterToNative(FilterPredicate predicate) {
    String predicateStr = ParquetFilterPredicateConvertor.toJsonString(predicate);
    ParquetReaderJNI.setFilterStr(reader, predicateStr);
  }

  @Override
  public void setFilter(FilterPredicate predicate) {
    this.filterPredicate = predicate;
  }

  private void setAggToNative(String aggExpr) {
    ParquetReaderJNI.setAggStr(reader, aggExpr);
  }

  @Override
  public void setAgg(String aggExpresion) {
    this.aggExpr = aggExpresion;
  }

  @Override
  public void setPlasmaCacheRedis(String host, int port, String password) {
    ParquetReaderJNI.setPlasmaCacheRedis(reader, host, port, password);
  }

  // TODO: how about remote reader?
  @Override
  public void setPartitionInfo(StructType partitionColumns, InternalRow partitionValues) {
    this.partitionColumns = partitionColumns;
    this.partitionValues = partitionValues;
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

  private void dumpPartitionColumns(int rowsRead) {
    int startId = columnVectors.length - partitionColumns.length();
    for (int i = 0 ; i < partitionColumns.length(); i++) {
      DataType dt = columnVectors[startId + i].dataType();
      long nullPtr = nullPtrs[startId + i];
      long dataPtr = bufferPtrs[startId + i];
      byte isNull = partitionValues.isNullAt(i) ? (byte)0 : (byte)1;
      for (int j = 0; j < rowsRead; j++ ) {
        Platform.putByte(null, nullPtr + j, isNull);
        if (dt == DataTypes.IntegerType) {
          Platform.putInt(null, dataPtr + j * 4, partitionValues.getInt(i));
        } else if(dt == DataTypes.LongType) {
          Platform.putLong(null, dataPtr + j * 8, partitionValues.getLong(i));
        } else {
          throw new UnsupportedOperationException("Unsupported partition key type: "
                  + dt.typeName());
        }
      }
    }
  }

  @Override
  public boolean nextBatch() {
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

    // native reader will read random value into partitioned column buffers, so we will re-fill
    if (partitionColumns != null) {
      dumpPartitionColumns(rowsRead);
    }
    // build columnVectors by buffers and nulls
    columnarBatch.setNumRows(0);

    columnarBatch.setNumRows(rowsRead);
    readTime += (System.nanoTime() - before);
    return true;
  }

  @Override
  void initBatch() {
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
      } else if(type instanceof DecimalType) {
        bufferPtr = Platform.allocateMemory(batchSize * 16);
      } else {
        // TODO: will add byte and short type. Not sure about Map
        throw new UnsupportedOperationException("Type not support yet");
      }
      ((NativeColumnVector)columnVectors[i]).set(bufferPtr, nullPtr, batchSize);
      bufferPtrs[i] = bufferPtr;
      nullPtrs[i] = nullPtr;
    }

    columnarBatch = new ColumnarBatch(columnVectors);
  }
}
