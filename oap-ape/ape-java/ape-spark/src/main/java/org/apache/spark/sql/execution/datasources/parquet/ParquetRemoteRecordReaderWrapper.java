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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.intel.ape.service.netty.NettyMessage;
import com.intel.ape.service.params.ParquetReaderInitParams;
import com.intel.ape.util.ParquetFilterPredicateConvertor;
import com.intel.oap.ape.service.netty.NettyParquetRequestHelper;
import com.intel.oap.ape.service.netty.client.ParquetDataRequestClient;
import io.netty.buffer.ByteBuf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.execution.vectorized.RemoteColumnVector;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class ParquetRemoteRecordReaderWrapper extends ParquetRecordReaderWrapper {
  private static final Logger LOG =
          LoggerFactory.getLogger(ParquetRemoteRecordReaderWrapper.class);

  String redisHost;
  int redisPort;
  String redisPassword;

  String predicateStr;
  String aggStr;

  private RemoteColumnVector remoteColumnVector;
  private NettyParquetRequestHelper parquetRequestHelper;
  private ParquetDataRequestClient requestClient;
  private Map<Integer, NettyMessage.ReadBatchResponse> responses;
  private boolean isAggregatePushedDown = false;
  public ParquetRemoteRecordReaderWrapper(int capacity) {
    super(capacity);
    responses = new HashMap<>();
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
    // parse type lengths in requested schema
    List<Integer> typeSizes = new ArrayList<>();
    List<Boolean> variableLengthFlags = new ArrayList<>();
    parseRequestedTypeLengths(sparkSchema.fields(), typeSizes, variableLengthFlags);
    // build params for remote parquet reader
    ParquetReaderInitParams params = new ParquetReaderInitParams(
            fileName,
            hdfsHost,
            hdfsPort,
            sparkSchema.json(),
            inputSplitRowGroupStartIndex,
            inputSplitRowGroupNum,
            typeSizes,
            variableLengthFlags,
            batchSize,
            cacheEnabled,
            preBufferEnabled,
            false
    );
    // set cache locality
    ParquetReaderInitParams.CacheLocalityStorage cacheLocalityStorage;
    if (redisEnabled) {
      cacheLocalityStorage = new ParquetReaderInitParams.CacheLocalityStorage(
              redisHost, redisPort, redisPassword);
      params.setCacheLocalityStorage(cacheLocalityStorage);
    }
    if (predicateStr != null) {
      params.setFilterPredicate(predicateStr);
    }

    // push down aggregates
    if (aggStr != null) {
      params.setAggregateExpression(aggStr);
      isAggregatePushedDown = true;
    }

    // create request client
    try {
      parquetRequestHelper = new NettyParquetRequestHelper(configuration);
      org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(split.getPath().toUri());
      requestClient = parquetRequestHelper.createRequestClient(
              file, split.getStart(), split.getLength());
      requestClient.initRemoteParquetReader(params);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    LOG.info("remote parquet reader initialized, plasma cache: {},  "
                    + "cache locality: {}, pre buffer: {}",
            cacheEnabled, redisEnabled, preBufferEnabled);
  }

  private void parseRequestedTypeLengths(StructField[] fields,
                                         List<Integer> typeSizes,
                                         List<Boolean> variableLengthFlags) {
    for (int i = 0; i < fields.length; i++) {
      DataType type = fields[i].dataType();
      if (type instanceof BooleanType) {
        typeSizes.add(1);
        variableLengthFlags.add(false);
      } else if (type instanceof DateType || type instanceof IntegerType
              || type instanceof FloatType) {
        typeSizes.add(4);
        variableLengthFlags.add(false);
      } else if (type instanceof LongType || type instanceof DoubleType
              || type instanceof TimestampType) {
        typeSizes.add(8);
        variableLengthFlags.add(false);
      } else if (type instanceof BinaryType || type instanceof StringType) {
        typeSizes.add(16);
        variableLengthFlags.add(true);
      } else if (DecimalType.is32BitDecimalType(type)) {
        typeSizes.add(4);
        variableLengthFlags.add(false);
      } else if (DecimalType.is64BitDecimalType(type)) {
        typeSizes.add(8);
        variableLengthFlags.add(false);
      } else if(type instanceof DecimalType) {
        typeSizes.add(16);
        variableLengthFlags.add(false);
      } else {
        // TODO: will add byte and short type. Not sure about Map
        throw new UnsupportedOperationException("Type not support yet");
      }
    }
  }

  @Override
  public void setCacheEnabled(boolean cacheEnabled) {
    this.cacheEnabled = cacheEnabled;
  }

  @Override
  public void setPreBufferEnabled(boolean preBufferEnabled) {
    this.preBufferEnabled = preBufferEnabled;
  }

  @Override
  public void setFilter(FilterPredicate predicate) {
    this.predicateStr = ParquetFilterPredicateConvertor.toJsonString(predicate);
  }

  @Override
  public void setAgg(String aggExpresion) {
    this.aggStr = aggExpresion;
  }

  @Override
  public void setPlasmaCacheRedis(String host, int port, String password) {
    this.redisHost = host;
    this.redisPort = port;
    this.redisPassword = password;
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
    // TODO: free remoteColumnarBatch, necessary?
  }

  @Override
  public boolean nextBatch() {
    if (requestClient == null) {
      return false;
    }
    // release buffers in used response
    int trackingId = ((RemoteColumnVector)columnVectors[0]).getTrackingId();
    if (trackingId > 0) {
      NettyMessage.ReadBatchResponse response = responses.remove(trackingId);
      response.releaseBuffers();
    }
    // read next batch
    int rowsRead = 0;
    try {
      NettyMessage.ReadBatchResponse response = requestClient.nextBatch();
      int batchSequenceId = response.getSequenceId();
      responses.put(batchSequenceId, response);
      rowsRead = response.getRowCount();
      // parse response data
      for (int i = 0; i < columnVectors.length; i++) {
        // check if column elements have variable lengths
        ByteBuf elementLengthBuf = null;
        if (response.getCompositeFlags()[i]) {
          // slice buffer of element lengths for current column
          elementLengthBuf = response.getCompositedElementLengths()
                  .readRetainedSlice(rowsRead * 4);
        }
        // set data into vector
        if ((columnVectors[i] instanceof RemoteColumnVector)) {
          RemoteColumnVector remoteColumnVector = (RemoteColumnVector) columnVectors[i];
          remoteColumnVector.setTrackingId(batchSequenceId);
          remoteColumnVector.setBuffers(
                  response.getDataBuffers()[i],
                  response.getNullBuffers()[i],
                  elementLengthBuf
          );
        } else {
          throw new RuntimeException(
                  "Invalid invoke of nextBatch on non-remote vectors.");
        }
      }
      return rowsRead > 0 || response.hasNextBatch();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  void initBatch() {
    // allocate buffers here according schema
    StructField[] fileds = sparkSchema.fields();
    columnVectors = new RemoteColumnVector[fileds.length];
    for (int i = 0; i < fileds.length; i++) {
      DataType type = fileds[i].dataType();
      if (type instanceof BinaryType || type instanceof StringType
              || type instanceof BooleanType || type instanceof DateType
              || type instanceof IntegerType || type instanceof FloatType
              || type instanceof LongType || type instanceof DoubleType
              || type instanceof TimestampType || DecimalType.is32BitDecimalType(type)
              ||  DecimalType.is64BitDecimalType(type) || type instanceof DecimalType) {
        columnVectors[i] = new RemoteColumnVector(batchSize, type);
      } else {
        // TODO: will add byte and short type. Not sure about Map
        throw new UnsupportedOperationException("Type not support yet");
      }
    }
    columnarBatch = new ColumnarBatch(columnVectors);
  }
}
