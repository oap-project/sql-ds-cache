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

package org.apache.flink.formats.parquet.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.intel.ape.ParquetReaderJNI;
import com.intel.ape.util.ParquetFilterPredicateConvertor;
import com.intel.oap.fs.hadoop.ape.hcfs.Constants;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.vector.nativevector.AbstractNativeVector;
import org.apache.flink.formats.parquet.vector.nativevector.NativeBooleanVector;
import org.apache.flink.formats.parquet.vector.nativevector.NativeBytesVector;
import org.apache.flink.formats.parquet.vector.nativevector.NativeDoubleVector;
import org.apache.flink.formats.parquet.vector.nativevector.NativeFixedBytesVector;
import org.apache.flink.formats.parquet.vector.nativevector.NativeFloatVector;
import org.apache.flink.formats.parquet.vector.nativevector.NativeIntVector;
import org.apache.flink.formats.parquet.vector.nativevector.NativeLongVector;
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

public class ParquetNativeRecordReaderWrapper implements ParquetRecordReaderWrapper {

    private static final Logger LOG =
            LoggerFactory.getLogger(ParquetNativeRecordReaderWrapper.class);

    long reader = 0; // pointer of native reader

    int batchSize;
    int rowsRead;

    private final List<Long> allocatedMemory = new ArrayList<>();

    private boolean isAggregatePushedDown = false;

    private long lastBatchTime = 0L;
    private long batchProcessingTime = 0L;
    private long batchRequestingTime = 0L;

    public ParquetNativeRecordReaderWrapper(int capacity) {
        this.batchSize = capacity;
    }

    @Override
    public void initialize(
            Configuration hadoopConfig,
            RowType projectedType,
            FileSourceSplit split,
            FilterPredicate filterPredicate,
            String aggStr) throws IOException {
        initialize(hadoopConfig, projectedType, split);
        setFilterPredicate(filterPredicate);
        setAggStr(aggStr);
    }

    protected void initialize(
            Configuration hadoopConfig,
            RowType projectedType,
            FileSourceSplit split) throws IOException {

        // get required row groups
        ParquetUtils.RequiredRowGroups requiredRowGroups =
                ParquetUtils.getRequiredSplitRowGroups(split, hadoopConfig);

        // get file info
        final Path filePath = split.path();
        final String fileName = filePath.toUri().getRawPath();
        // this string is like hdfs://host:port
        final String hdfs = hadoopConfig.get("fs.defaultFS");
        String[] res = hdfs.split(":");
        String hdfsHost = filePath.toUri().getHost() != null ?
                filePath.toUri().getHost() : res[1].substring(2);
        int hdfsPort = filePath.toUri().getPort() != -1 ?
                filePath.toUri().getPort() : Integer.parseInt(res[2]);

        List<String> fieldTypeList = new ArrayList<String>();
        List<String> projectedFields = projectedType.getFieldNames();
        LogicalType[] projectedTypes = projectedType.getChildren().toArray(new LogicalType[0]);
        for (LogicalType type : projectedTypes) {
            fieldTypeList.add(type.getTypeRoot().name());
        }

        // schema
        RequestedSchemaJsonConvertor message = new RequestedSchemaJsonConvertor(fieldTypeList, projectedFields);

        // cache configuration
        boolean plasmaCacheEnabled =
                hadoopConfig.getBoolean("fs.ape.reader.plasmaCacheEnabled", false);
        boolean plasmaCacheAsync =
                hadoopConfig.getBoolean("fs.ape.reader.plasmaCacheAsync", false);
        boolean preBufferEnabled =
                hadoopConfig.getBoolean("fs.ape.reader.preBufferEnabled", false);

        // init native reader
        reader = ParquetReaderJNI.init(
                fileName, hdfsHost, hdfsPort, message.toJson(),
                requiredRowGroups.getStartIndex(), requiredRowGroups.getNumber(),
                plasmaCacheEnabled, preBufferEnabled, plasmaCacheAsync);

        // cache locality
        boolean cacheLocalityEnabled =
                hadoopConfig.getBoolean("fs.ape.reader.cacheLocalityEnabled", false);
        if (cacheLocalityEnabled) {
            ParquetReaderJNI.setPlasmaCacheRedis(
                    reader,
                    hadoopConfig.get(Constants.CONF_KEY_FS_APE_HCFS_REDIS_HOST,
                            Constants.DEFAULT_REDIS_HOST),
                    hadoopConfig.getInt(Constants.CONF_KEY_FS_APE_HCFS_REDIS_PORT,
                            Constants.DEFAULT_REDIS_PORT),
                    hadoopConfig.get(Constants.CONF_KEY_FS_APE_HCFS_REDIS_AUTH,
                            Constants.DEFAULT_REDIS_AUTH));
        }

        LOG.info("native parquet reader initialized, plasma cache: {},  "
                        + "cache locality: {}, pre buffer: {}",
                plasmaCacheEnabled, cacheLocalityEnabled, preBufferEnabled);

    }

    public long getReader() {
        return this.reader;
    }

    public WritableColumnVector[] initBatch(
            MessageType requestSchema, int batchSize, RowType projectedType) {
        WritableColumnVector[] columns = new WritableColumnVector[projectedType.getFieldCount()];

        for (int i = 0; i < projectedType.getFieldCount(); i++) {
            PrimitiveType primitiveType = null;
            PrimitiveType.PrimitiveTypeName typeName = null;

            if (!isAggregatePushedDown) {
                primitiveType = requestSchema.getColumns().get(i).getPrimitiveType();
                typeName = primitiveType.getPrimitiveTypeName();
            }
            LogicalType fieldType = projectedType.getTypeAt(i);

            long nullPtr = Platform.allocateMemory(batchSize);
            allocatedMemory.add(nullPtr);

            long bufferPtr;
            int typeSize = 1;
            switch (fieldType.getTypeRoot()) {
                case BOOLEAN:
                    checkArgument(typeName == null ||
                                    typeName == PrimitiveType.PrimitiveTypeName.BOOLEAN,
                            "Unexpected type: %s", typeName);
                    NativeBooleanVector booleanVector =
                            new NativeBooleanVector(batchSize, typeSize);
                    bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                    booleanVector.setPtr(bufferPtr, nullPtr, batchSize);
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
                    NativeIntVector intVector = new NativeIntVector(batchSize, typeSize);
                    bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                    intVector.setPtr(bufferPtr, nullPtr, batchSize);
                    columns[i] = intVector;
                    break;
                case BIGINT:
                    checkArgument(typeName == null ||
                                    typeName == PrimitiveType.PrimitiveTypeName.INT64,
                            "Unexpected type: %s", typeName);
                    typeSize = 8;
                    NativeLongVector longVector = new NativeLongVector(batchSize, typeSize);
                    bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                    longVector.setPtr(bufferPtr, nullPtr, batchSize);
                    columns[i] = longVector;
                    break;
                case FLOAT:
                    checkArgument(typeName == null ||
                                    typeName == PrimitiveType.PrimitiveTypeName.FLOAT,
                            "Unexpected type: %s", typeName);
                    typeSize = 4;
                    NativeFloatVector floatVector = new NativeFloatVector(batchSize, typeSize);
                    bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                    floatVector.setPtr(bufferPtr, nullPtr, batchSize);
                    columns[i] = floatVector;
                    break;
                case DOUBLE:
                    checkArgument(typeName == null ||
                                    typeName == PrimitiveType.PrimitiveTypeName.DOUBLE,
                            "Unexpected type: %s", typeName);
                    typeSize = 8;
                    NativeDoubleVector doubleVector = new NativeDoubleVector(batchSize, typeSize);
                    bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                    doubleVector.setPtr(bufferPtr, nullPtr, batchSize);
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
                    NativeBytesVector bytesVector = new NativeBytesVector(batchSize, typeSize);
                    bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                    bytesVector.setPtr(bufferPtr, nullPtr, batchSize);
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
                        NativeIntVector decimal32Vector = new NativeIntVector(batchSize, typeSize);
                        bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                        decimal32Vector.setPtr(bufferPtr, nullPtr, batchSize);
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
                        NativeLongVector decimal64Vector =
                                new NativeLongVector(batchSize, typeSize);
                        bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                        decimal64Vector.setPtr(bufferPtr, nullPtr, batchSize);
                        columns[i] = decimal64Vector;
                    } else {
                        typeSize = 16;
                        NativeFixedBytesVector decimalBytesVector =
                                new NativeFixedBytesVector(batchSize, typeSize);
                        bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                        decimalBytesVector.setPtr(bufferPtr, nullPtr, batchSize);
                        columns[i] = decimalBytesVector;
                    }
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                default:
                    throw new UnsupportedOperationException(fieldType + " is not supported now.");
            }

            allocatedMemory.add(bufferPtr);
        }
        return columns;
    }

    public int getRowsRead() {
        return rowsRead;
    }

    public void close() {
        // close native reader
        if (reader != 0) {
            ParquetReaderJNI.close(reader);
        }

        // release memory
        for (Long addr : allocatedMemory) {
            Platform.freeMemory(addr);
        }
        allocatedMemory.clear();

        LOG.info("Requesting batches takes: {} ms", batchRequestingTime / 1_000_000);
        LOG.info("Processing batches takes: {} ms", batchProcessingTime / 1_000_000);
    }

    public boolean nextBatch(WritableColumnVector[] columns) {
        if (reader == 0) {
            return false;
        }

        if (lastBatchTime > 0) {
            final long duration = (System.nanoTime() - lastBatchTime);
            batchProcessingTime += duration;
        }

        long[] bufferPtrs = new long[columns.length];
        long[] nullPtrs = new long[columns.length];
        int index = 0;

        for (WritableColumnVector column : columns) {
            if (column instanceof AbstractNativeVector) {
                bufferPtrs[index] = ((AbstractNativeVector) column).getBufferPtr();
                nullPtrs[index] = ((AbstractNativeVector) column).getNullPtr();
            } else {
                throw new RuntimeException("Invalid invoke of nextBatch on non-native vectors.");
            }
            index++;
        }

        final long start = System.nanoTime();

        rowsRead = ParquetReaderJNI.readBatch(reader, batchSize, bufferPtrs, nullPtrs);

        final long duration = (System.nanoTime() - start);
        batchRequestingTime += duration;

        lastBatchTime = System.nanoTime();

        if (rowsRead < 0) {
            return false;
        }

        return true;
    }

    public boolean skipNextRowGroup() {
        return ParquetReaderJNI.skipNextRowGroup(reader);
    }

    protected void setFilterPredicate(FilterPredicate filterPredicate) {
        if (filterPredicate != null) {
            String predicateStr = ParquetFilterPredicateConvertor.toJsonString(filterPredicate);
            ParquetReaderJNI.setFilterStr(reader, predicateStr);
        }
    }

    protected void setAggStr(String aggStr) {
        if (aggStr != null && !aggStr.isEmpty()) {
            ParquetReaderJNI.setAggStr(reader, aggStr);
            isAggregatePushedDown = true;
        }
    }

}
