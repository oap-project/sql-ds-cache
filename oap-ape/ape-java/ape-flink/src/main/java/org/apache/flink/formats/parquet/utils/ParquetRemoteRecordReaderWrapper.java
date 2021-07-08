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

package org.apache.flink.formats.parquet.utils;

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
import com.intel.oap.fs.hadoop.ape.hcfs.Constants;

import io.netty.buffer.ByteBuf;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.vector.remotevector.AbstractRemoteVector;
import org.apache.flink.formats.parquet.vector.remotevector.RemoteBooleanVector;
import org.apache.flink.formats.parquet.vector.remotevector.RemoteBytesVector;
import org.apache.flink.formats.parquet.vector.remotevector.RemoteDoubleVector;
import org.apache.flink.formats.parquet.vector.remotevector.RemoteFixedBytesVector;
import org.apache.flink.formats.parquet.vector.remotevector.RemoteFloatVector;
import org.apache.flink.formats.parquet.vector.remotevector.RemoteIntVector;
import org.apache.flink.formats.parquet.vector.remotevector.RemoteLongVector;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.vector.writable.WritableColumnVector;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.parquet.Preconditions.checkArgument;

/**
 * Wrapper class for parquet data loading from remote servers.
 */
public class ParquetRemoteRecordReaderWrapper implements ParquetRecordReaderWrapper {

    private static final Logger LOG =
            LoggerFactory.getLogger(ParquetRemoteRecordReaderWrapper.class);

    private final int batchSize;
    private int rowsRead;

    private boolean isAggregatePushedDown = false;

    private NettyParquetRequestHelper parquetRequestHelper;
    private ParquetDataRequestClient requestClient;
    private final Map<Integer, NettyMessage.ReadBatchResponse> responses;

    private long lastBatchTime = 0L;
    private long batchProcessingTime = 0L;
    private long batchRequestingTime = 0L;

    public ParquetRemoteRecordReaderWrapper(int batchSize) {
        this.batchSize = batchSize;
        responses = new HashMap<>();
    }

    public void initialize(
            Configuration hadoopConfig,
            RowType projectedType,
            FileSourceSplit split,
            FilterPredicate filterPredicate,
            String aggStr) throws IOException {

        // get required row groups
        ParquetUtils.RequiredRowGroups requiredRowGroups =
                ParquetUtils.getRequiredSplitRowGroups(split, hadoopConfig);

        // get hdfs file info
        final Path filePath = split.path();
        final String fileName = filePath.toUri().getRawPath();
        // this string is like hdfs://host:port
        final String hdfs = hadoopConfig.get("fs.defaultFS");
        String[] res = hdfs.split(":");
        String hdfsHost = filePath.toUri().getHost() != null ?
                filePath.toUri().getHost() : res[1].substring(2);
        int hdfsPort = filePath.toUri().getPort() != -1 ?
                filePath.toUri().getPort() : Integer.parseInt(res[2]);

        // get required columns
        List<String> projectedFields = projectedType.getFieldNames();
        List<String> fieldTypeList = new ArrayList<>();
        LogicalType[] projectedTypes = projectedType.getChildren().toArray(new LogicalType[0]);
        for (LogicalType type : projectedTypes) {
            fieldTypeList.add(type.getTypeRoot().name());
        }

        // schema in json format
        String jsonSchema = new RequestedSchemaJsonConvertor(fieldTypeList, projectedFields).toJson();

        // cache configuration
        boolean plasmaCacheEnabled =
                hadoopConfig.getBoolean("fs.ape.reader.plasmaCacheEnabled", false);
        boolean plasmaCacheAsync =
                hadoopConfig.getBoolean("fs.ape.reader.plasmaCacheAsync", false);
        boolean preBufferEnabled =
                hadoopConfig.getBoolean("fs.ape.reader.preBufferEnabled", false);
        boolean cacheLocalityEnabled =
                hadoopConfig.getBoolean("fs.ape.reader.cacheLocalityEnabled", false);
        boolean remoteReaderCompressEnabled =
                hadoopConfig.getBoolean("fs.ape.reader.remote.compress", false);

        // parse type lengths in requested schema
        List<Integer> typeSizes = new ArrayList<>();
        List<Boolean> variableLengthFlags = new ArrayList<>();
        parseRequestedTypeLengths(projectedType, typeSizes, variableLengthFlags);

        // build params for remote parquet reader
        ParquetReaderInitParams params = new ParquetReaderInitParams(
                fileName,
                hdfsHost,
                hdfsPort,
                jsonSchema,
                requiredRowGroups.getStartIndex(),
                requiredRowGroups.getNumber(),
                typeSizes,
                variableLengthFlags,
                batchSize,
                plasmaCacheEnabled,
                preBufferEnabled,
                plasmaCacheAsync,
                remoteReaderCompressEnabled
        );

        // set cache locality
        ParquetReaderInitParams.CacheLocalityStorage cacheLocalityStorage;
        if (cacheLocalityEnabled) {
            cacheLocalityStorage = new ParquetReaderInitParams.CacheLocalityStorage(
                    hadoopConfig.get(Constants.CONF_KEY_FS_APE_HCFS_REDIS_HOST,
                            Constants.DEFAULT_REDIS_HOST),
                    hadoopConfig.getInt(Constants.CONF_KEY_FS_APE_HCFS_REDIS_PORT,
                            Constants.DEFAULT_REDIS_PORT),
                    hadoopConfig.get(Constants.CONF_KEY_FS_APE_HCFS_REDIS_AUTH,
                            Constants.DEFAULT_REDIS_AUTH)
            );
            params.setCacheLocalityStorage(cacheLocalityStorage);
        }

        // push down filters
        if (filterPredicate != null) {
            String predicateStr = ParquetFilterPredicateConvertor.toJsonString(filterPredicate);
            params.setFilterPredicate(predicateStr);
        }

        // push down aggregates
        if (aggStr != null) {
            params.setAggregateExpression(aggStr);
            isAggregatePushedDown = true;
        }

        // create request client
        try {
            parquetRequestHelper = new NettyParquetRequestHelper(hadoopConfig);
            org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(split.path().toUri());
            requestClient = parquetRequestHelper.createRequestClient(
                    file, split.offset(), split.length());
            requestClient.initRemoteParquetReader(params);
            requestClient.sendReadBatchRequest(3);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        LOG.info("remote parquet reader initialized, plasma cache: {},  "
                        + "cache locality: {}, pre buffer: {}",
                plasmaCacheEnabled, cacheLocalityEnabled, preBufferEnabled);
    }

    private void parseRequestedTypeLengths(
            RowType projectedType,
            List<Integer> typeSizes,
            List<Boolean> variableLengthFlags) {
        for (int i = 0; i < projectedType.getFieldCount(); i++) {
            LogicalType fieldType = projectedType.getTypeAt(i);
            int typeSize = 1;
            switch (fieldType.getTypeRoot()) {
                case BOOLEAN:
                    typeSizes.add(typeSize);
                    variableLengthFlags.add(false);
                    break;
                case DATE:
                case SMALLINT:
                case INTEGER:
                case TIME_WITHOUT_TIME_ZONE:
                case FLOAT:
                    typeSize = 4;
                    typeSizes.add(typeSize);
                    variableLengthFlags.add(false);
                    break;
                case BIGINT:
                case DOUBLE:
                    typeSize = 8;
                    typeSizes.add(typeSize);
                    variableLengthFlags.add(false);
                    break;
                case CHAR:
                case VARCHAR:
                case BINARY:
                case VARBINARY:
                    typeSize = 16;
                    typeSizes.add(typeSize);
                    variableLengthFlags.add(true);
                    break;
                case DECIMAL:
                    DecimalType decimalType = (DecimalType) fieldType;
                    if (DecimalDataUtils.is32BitDecimal(decimalType.getPrecision())) {
                        typeSize = 4;
                        typeSizes.add(typeSize);
                        variableLengthFlags.add(false);
                    } else if (DecimalDataUtils.is64BitDecimal(decimalType.getPrecision())) {
                        typeSize = 8;
                        typeSizes.add(typeSize);
                        variableLengthFlags.add(false);
                    } else {
                        typeSize = 16;
                        typeSizes.add(typeSize);
                        variableLengthFlags.add(false);
                    }
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                default:
                    throw new UnsupportedOperationException(fieldType + " is not supported now.");
            }
        }
    }

    public WritableColumnVector[] initBatch(
            MessageType requestSchema, int batchSize, RowType projectedType) {
        WritableColumnVector[] columns = new WritableColumnVector[projectedType.getFieldCount()];

        for (int i = 0; i < projectedType.getFieldCount(); i++) {
            PrimitiveType primitiveType = null;
            PrimitiveType.PrimitiveTypeName typeName = null;

            // will not check types in schemas of file and request when aggregates are pushed down
            if (!isAggregatePushedDown) {
                primitiveType = requestSchema.getColumns().get(i).getPrimitiveType();
                typeName = primitiveType.getPrimitiveTypeName();
            }
            LogicalType fieldType = projectedType.getTypeAt(i);

            int typeSize = 1;
            switch (fieldType.getTypeRoot()) {
                case BOOLEAN:
                    checkArgument(typeName == null ||
                                    typeName == PrimitiveType.PrimitiveTypeName.BOOLEAN,
                            "Unexpected type: %s", typeName);
                    RemoteBooleanVector booleanVector =
                            new RemoteBooleanVector(batchSize, typeSize);
                    columns[i] = booleanVector;
                    break;
                case DATE:
                case SMALLINT:
                case INTEGER:
                case TIME_WITHOUT_TIME_ZONE:
                    checkArgument(typeName == null ||
                                    typeName == PrimitiveType.PrimitiveTypeName.INT32,
                            "Unexpected type: %s", typeName);
                    typeSize = 4;
                    RemoteIntVector intVector = new RemoteIntVector(batchSize, typeSize);
                    columns[i] = intVector;
                    break;
                case BIGINT:
                    checkArgument(typeName == null ||
                                    typeName == PrimitiveType.PrimitiveTypeName.INT64,
                            "Unexpected type: %s", typeName);
                    typeSize = 8;
                    RemoteLongVector longVector = new RemoteLongVector(batchSize, typeSize);
                    columns[i] = longVector;
                    break;
                case FLOAT:
                    checkArgument(typeName == null ||
                                    typeName == PrimitiveType.PrimitiveTypeName.FLOAT,
                            "Unexpected type: %s", typeName);
                    typeSize = 4;
                    RemoteFloatVector floatVector = new RemoteFloatVector(batchSize, typeSize);
                    columns[i] = floatVector;
                    break;
                case DOUBLE:
                    checkArgument(typeName == null ||
                                    typeName == PrimitiveType.PrimitiveTypeName.DOUBLE,
                            "Unexpected type: %s", typeName);
                    typeSize = 8;
                    RemoteDoubleVector doubleVector = new RemoteDoubleVector(batchSize, typeSize);
                    columns[i] = doubleVector;
                    break;
                case CHAR:
                case VARCHAR:
                case BINARY:
                case VARBINARY:
                    checkArgument(typeName == null ||
                                    typeName == PrimitiveType.PrimitiveTypeName.BINARY,
                            "Unexpected type: %s", typeName);
                    typeSize = 16;
                    RemoteBytesVector bytesVector = new RemoteBytesVector(batchSize, typeSize);
                    columns[i] = bytesVector;
                    break;
                case DECIMAL:
                    DecimalType decimalType = (DecimalType) fieldType;
                    if (DecimalDataUtils.is32BitDecimal(decimalType.getPrecision())) {
                        checkArgument(
                            (typeName == null
                                || typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                                || typeName == PrimitiveType.PrimitiveTypeName.INT32
                            ) && (primitiveType == null
                                || primitiveType.getOriginalType() == OriginalType.DECIMAL
                            ),
                            "Unexpected type: %s", typeName);
                        typeSize = 4;
                        RemoteIntVector decimal32Vector = new RemoteIntVector(batchSize, typeSize);
                        columns[i] = decimal32Vector;
                    } else if (DecimalDataUtils.is64BitDecimal(decimalType.getPrecision())) {
                        checkArgument(
                            (typeName == null
                                || typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                                || typeName == PrimitiveType.PrimitiveTypeName.INT64
                            ) && (primitiveType == null
                                || primitiveType.getOriginalType() == OriginalType.DECIMAL
                            ),
                            "Unexpected type: %s", typeName);

                        typeSize = 8;
                        RemoteLongVector decimal64Vector =
                                new RemoteLongVector(batchSize, typeSize);
                        columns[i] = decimal64Vector;
                    } else {
                        typeSize = 16;
                        RemoteFixedBytesVector decimalBytesVector =
                                new RemoteFixedBytesVector(batchSize, typeSize);
                        columns[i] = decimalBytesVector;
                    }
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                default:
                    throw new UnsupportedOperationException(fieldType + " is not supported now.");
            }
        }
        return columns;
    }

    public boolean nextBatch(WritableColumnVector[] columns) {
        if (requestClient == null || !requestClient.hasNextBatch()) {
            return false;
        }

        if (lastBatchTime > 0) {
            final long duration = (System.nanoTime() - lastBatchTime);
            batchProcessingTime += duration;
        }

        // release buffers in used response
        int trackingId = columns.length > 0 ?
                ((AbstractRemoteVector) columns[0]).getTrackingId() : -1;
        if (trackingId >= 0) {
            NettyMessage.ReadBatchResponse response = responses.remove(trackingId);
            response.releaseBuffers();
        }

        // read next batch
        try {
            final long start = System.nanoTime();

            NettyMessage.ReadBatchResponse response = requestClient.nextBatch();
            if (response.hasNextBatch()) {
                requestClient.sendReadBatchRequest();
            }

            final long duration = (System.nanoTime() - start);
            batchRequestingTime += duration;

            int batchSequenceId = response.getSequenceId();
            responses.put(batchSequenceId, response);
            rowsRead = response.getRowCount();

            // parse response data
            for (int i = 0; i < columns.length; i++) {
                WritableColumnVector column = columns[i];

                if (column instanceof AbstractRemoteVector) {
                    // check if column elements have variable lengths
                    ByteBuf elementLengthBuf = null;
                    if (response.getCompositeFlags()[i]) {
                        // slice buffer of element lengths for current column
                        elementLengthBuf = response.getCompositedElementLengths()
                                .readRetainedSlice(rowsRead * 4);
                    }

                    // set data into vector
                    AbstractRemoteVector remoteVector = ((AbstractRemoteVector) column);
                    remoteVector.setTrackingId(batchSequenceId);
                    remoteVector.setBuffers(
                            response.getDataBuffers()[i],
                            response.getNullBuffers()[i],
                            elementLengthBuf
                    );
                } else {
                    throw new RuntimeException(
                            "Invalid invoke of nextBatch on non-remote vectors.");
                }
            }

            lastBatchTime = System.nanoTime();

            return rowsRead > 0 || response.hasNextBatch();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public boolean skipNextRowGroup() {
        try {
            requestClient.skipNextRowGroup();
            return true;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public int getRowsRead() {
        return rowsRead;
    }

    public void close() {
        try {
            if (requestClient != null) {
                requestClient.close();
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            if (parquetRequestHelper != null) {
                parquetRequestHelper.shutdownNettyClient();
            }
        }

        for (NettyMessage.ReadBatchResponse response: responses.values()) {
            response.releaseBuffers();
        }
        responses.clear();

        LOG.info("Requesting batches takes: {} ms", batchRequestingTime / 1_000_000);
        LOG.info("Processing batches takes: {} ms", batchProcessingTime / 1_000_000);

    }
}
