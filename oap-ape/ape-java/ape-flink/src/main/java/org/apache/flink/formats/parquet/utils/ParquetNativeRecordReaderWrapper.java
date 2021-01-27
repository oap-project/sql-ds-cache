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

import static org.apache.parquet.Preconditions.checkArgument;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.intel.ape.ParquetReaderJNI;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.vector.nativevector.NativeBoolenVector;
import org.apache.flink.formats.parquet.vector.nativevector.NativeBytesVector;
import org.apache.flink.formats.parquet.vector.nativevector.NativeDoubleVector;
import org.apache.flink.formats.parquet.vector.nativevector.NativeFloatVector;
import org.apache.flink.formats.parquet.vector.nativevector.NativeIntVector;
import org.apache.flink.formats.parquet.vector.nativevector.NativeLongVector;
import org.apache.flink.formats.parquet.vector.nativevector.NativeVector;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.vector.writable.WritableColumnVector;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetNativeRecordReaderWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetNativeRecordReaderWrapper.class);

    long reader = 0;
    int batchSize;
    int rowsRead;

    private int inputSplitRowGroupStartIndex = 0;
    private int inputSplitRowGroupNum = 0;

    private final List<Long> allocatedMemory = new ArrayList<>();

    public ParquetNativeRecordReaderWrapper(int capacity) {
        this.batchSize = capacity;
    }

    public long initialize(SerializableConfiguration hadoopConfig, RowType projectedType, FileSourceSplit split)
            throws IOException {

        final Path filePath = split.path();
        getRequiredSplitRowGroup(split, hadoopConfig.conf());
        final String fileName = filePath.toUri().getRawPath();
        final String hdfs = hadoopConfig.conf().get("fs.defaultFS"); // this string is like hdfs://host:port
        String[] res = hdfs.split(":");
        String hdfsHost = filePath.toUri().getHost() != null ? filePath.toUri().getHost() : res[1].substring(2);
        int hdfsPort = filePath.toUri().getPort() != -1 ? filePath.toUri().getPort() : Integer.parseInt(res[2]);

        List<String> fieldTypeList = new ArrayList<String>();
        List<String> projectedFields = projectedType.getFieldNames();
        LogicalType[] projectedTypes = projectedType.getChildren().toArray(new LogicalType[0]);
        for (int i = 0; i < projectedTypes.length; i++) {
            fieldTypeList.add(projectedType.getTypeRoot().name());
        }

        ConvertToJson message = new ConvertToJson(fieldTypeList, projectedFields);
        reader = ParquetReaderJNI.init(
                fileName, hdfsHost, hdfsPort, message.toJson(), inputSplitRowGroupStartIndex, inputSplitRowGroupNum);

        LOG.debug("native parquet reader initialized");

        return reader;

    }

    public void getRequiredSplitRowGroup(FileSourceSplit split, Configuration configuration) throws IOException {
        long splitStart = split.offset();
        long splitSize = split.length();
        org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(split.path().toUri());

        ParquetMetadata footer = readFooter(configuration, file);
        List<BlockMetaData> blocks = footer.getBlocks();

        long currentOffset = 0L;
        long PARQUET_MAGIC_NUMBER = 4L;
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


    public long getReader() {
        return this.reader;
    }

    public WritableColumnVector[] initBatch(MessageType requestSchema, int batchSize, RowType projectedType) {
        WritableColumnVector[] columns = new WritableColumnVector[projectedType.getFieldCount()];

        for (int i = 0; i < projectedType.getFieldCount(); i++) {
            PrimitiveType primitiveType = requestSchema.getColumns().get(i).getPrimitiveType();
            PrimitiveType.PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();
            LogicalType fieldType = projectedType.getTypeAt(i);

            long nullPtr = Platform.allocateMemory(batchSize);
            allocatedMemory.add(nullPtr);

            long bufferPtr;
            int typeSize = 1;
            switch (fieldType.getTypeRoot()) {
                case BOOLEAN:
                    checkArgument(typeName == PrimitiveType.PrimitiveTypeName.BOOLEAN, "Unexpected type: %s", typeName);
                    NativeBoolenVector booleanVector = new NativeBoolenVector(batchSize, typeSize);
                    bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                    booleanVector.setPtr(bufferPtr, nullPtr, batchSize);
                    columns[i] = booleanVector;
                    break;
                case DATE:
                case SMALLINT:
                case INTEGER:
                case TIME_WITHOUT_TIME_ZONE:
                    checkArgument(typeName == PrimitiveType.PrimitiveTypeName.INT32, "Unexpected type: %s", typeName);
                    typeSize = 4;
                    NativeIntVector intVector = new NativeIntVector(batchSize, typeSize);
                    bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                    intVector.setPtr(bufferPtr, nullPtr, batchSize);
                    columns[i] = intVector;
                    break;
                case BIGINT:
                    checkArgument(typeName == PrimitiveType.PrimitiveTypeName.INT64, "Unexpected type: %s", typeName);
                    typeSize = 8;
                    NativeLongVector longVector = new NativeLongVector(batchSize, typeSize);
                    bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                    longVector.setPtr(bufferPtr, nullPtr, batchSize);
                    columns[i] = longVector;
                    break;
                case FLOAT:
                    checkArgument(typeName == PrimitiveType.PrimitiveTypeName.FLOAT, "Unexpected type: %s", typeName);
                    typeSize = 4;
                    NativeFloatVector floatVector = new NativeFloatVector(batchSize, typeSize);
                    bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                    floatVector.setPtr(bufferPtr, nullPtr, batchSize);
                    columns[i] = floatVector;
                    break;
                case DOUBLE:
                    checkArgument(typeName == PrimitiveType.PrimitiveTypeName.DOUBLE, "Unexpected type: %s", typeName);
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
                    checkArgument(typeName == PrimitiveType.PrimitiveTypeName.BINARY, "Unexpected type: %s", typeName);
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
                                (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                                        || typeName == PrimitiveType.PrimitiveTypeName.INT32)
                                        && primitiveType.getOriginalType() == OriginalType.DECIMAL,
                                "Unexpected type: %s", typeName);
                        typeSize = 4;
                        NativeIntVector decimal32Vector = new NativeIntVector(batchSize, typeSize);
                        bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                        decimal32Vector.setPtr(bufferPtr, nullPtr, batchSize);
                        columns[i] = decimal32Vector;
                    } else if (DecimalDataUtils.is64BitDecimal(decimalType.getPrecision())) {
                        checkArgument(
                                (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                                        || typeName == PrimitiveType.PrimitiveTypeName.INT64)
                                        && primitiveType.getOriginalType() == OriginalType.DECIMAL,
                                "Unexpected type: %s", typeName);

                        typeSize = 8;
                        NativeLongVector decimal64Vector = new NativeLongVector(batchSize, typeSize);
                        bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                        decimal64Vector.setPtr(bufferPtr, nullPtr, batchSize);
                        columns[i] = decimal64Vector;
                    } else {
                        throw new UnsupportedOperationException(fieldType + " is not supported now.");
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
    }

    public boolean nextBatch(WritableColumnVector[] columns) {
        if (reader == 0) {
            return false;
        }

        long[] bufferPtrs = new long[columns.length];
        long[] nullPtrs = new long[columns.length];
        int index = 0;

        for (WritableColumnVector column : columns) {
            if (column instanceof NativeVector) {
                bufferPtrs[index] = ((NativeVector) column).getBufferPtr();
                nullPtrs[index] = ((NativeVector) column).getNullPtr();
            } else {
                throw new RuntimeException("Invalid invoke of nextBatch on non-native vectors.");
            }
            index++;
        }

        rowsRead = ParquetReaderJNI.readBatch(reader, batchSize, bufferPtrs, nullPtrs);

        if (rowsRead < 0) {
            return false;
        }

        return true;
    }

    public boolean skipNextRowGroup() {
        return ParquetReaderJNI.skipNextRowGroup(reader);
    }

}
