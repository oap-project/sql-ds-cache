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
import org.apache.flink.formats.parquet.vector.nativevector.NativeShortVector;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.vector.writable.WritableColumnVector;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

public class ParquetNativeRecordReaderWrapper {

    long reader = 0;
    int batchSize = 0;

    long bufferPtrs[];
    long nullPtrs[];

    int rowRead;

    public ParquetNativeRecordReaderWrapper(int capacity) {
        this.batchSize = capacity;
    }

    // todo: get hdfs and schema info
    public long initialize(SerializableConfiguration hadoopConfig, RowType projectedType, FileSourceSplit split) {

        final Path filePath = split.path();
        final long splitOffset = split.offset();
        final long splitLength = split.length();
        final String fileName = filePath.toUri().getRawPath();
        final String hdfs = hadoopConfig.conf().get("fs.defaultFS"); // this string is like hdfs://host:port
        String[] res = hdfs.split(":");
        String hdfsHost = res[1].substring(2);
        int hdfsPort = Integer.parseInt(res[2]);

        List<String> fieldTypeList = new ArrayList<String>();
        List<String> projectedFields = projectedType.getFieldNames();
        LogicalType[] projectedTypes = projectedType.getChildren().toArray(new LogicalType[0]);
        for (int i = 0; i < projectedTypes.length; i++) {
            fieldTypeList.add(projectedType.getTypeRoot().name());
        }

        ConvertToJson message = new ConvertToJson(fieldTypeList, projectedFields);
        reader = ParquetReaderJNI.init(fileName, hdfsHost, hdfsPort, message.toString(), splitOffset, splitLength);
        return reader;

    }

    public long getReader() {
        return this.reader;
    }

    // todo: impl
    public WritableColumnVector[] initBatch(MessageType requestSchema, int batchSize, RowType projectedType) {

        WritableColumnVector[] columns = new WritableColumnVector[projectedType.getFieldCount()];

        for (int i = 0; i < projectedType.getFieldCount(); i++) {
            PrimitiveType primitiveType = requestSchema.getColumns().get(i).getPrimitiveType();
            PrimitiveType.PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();
            LogicalType fieldType = projectedType.getTypeAt(i);
            long nullPtr = Platform.allocateMemory(batchSize);
            long bufferPtr = 0;
            int typeSize = 16;
            nullPtrs[i] = nullPtr;

            switch (fieldType.getTypeRoot()) {
                case BOOLEAN:
                    checkArgument(typeName == PrimitiveType.PrimitiveTypeName.BOOLEAN, "Unexpected type: %s", typeName);
                    NativeBoolenVector boolenVector = new NativeBoolenVector(batchSize, typeSize);
                    bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                    boolenVector.setPtr(bufferPtr, nullPtr, batchSize);
                    columns[i] = boolenVector;
                    break;
                case DOUBLE:
                    checkArgument(typeName == PrimitiveType.PrimitiveTypeName.DOUBLE, "Unexpected type: %s", typeName);
                    NativeDoubleVector doubleVector = new NativeDoubleVector(batchSize, typeSize);
                    bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                    doubleVector.setPtr(bufferPtr, nullPtr, batchSize);
                    columns[i] = doubleVector;
                    break;
                case FLOAT:
                    checkArgument(typeName == PrimitiveType.PrimitiveTypeName.FLOAT, "Unexpected type: %s", typeName);
                    NativeFloatVector floatVector = new NativeFloatVector(batchSize, typeSize);
                    bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                    floatVector.setPtr(bufferPtr, nullPtr, batchSize);
                    columns[i] = floatVector;
                    break;
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                    checkArgument(typeName == PrimitiveType.PrimitiveTypeName.INT32, "Unexpected type: %s", typeName);
                    NativeIntVector intVector = new NativeIntVector(batchSize, typeSize);
                    bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                    intVector.setPtr(bufferPtr, nullPtr, batchSize);
                    columns[i] = intVector;
                case BIGINT:
                    checkArgument(typeName == PrimitiveType.PrimitiveTypeName.INT64, "Unexpected type: %s", typeName);
                    NativeLongVector longVector = new NativeLongVector(batchSize, typeSize);
                    bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                    longVector.setPtr(bufferPtr, nullPtr, batchSize);
                    columns[i] = longVector;
                    break;
                case SMALLINT:
                    checkArgument(typeName == PrimitiveType.PrimitiveTypeName.INT32, "Unexpected type: %s", typeName);
                    NativeShortVector shortVector = new NativeShortVector(batchSize, typeSize);
                    bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                    shortVector.setPtr(bufferPtr, nullPtr, batchSize);
                    columns[i] = shortVector;
                    break;
                case CHAR:
                case VARCHAR:
                case BINARY:
                case VARBINARY:
                    checkArgument(typeName == PrimitiveType.PrimitiveTypeName.BINARY, "Unexpected type: %s", typeName);
                    NativeBytesVector bytesVector = new NativeBytesVector(batchSize, typeSize);
                    bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                    bytesVector.setPtr(bufferPtr, nullPtr, batchSize);
                    columns[i] = bytesVector;
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    throw new UnsupportedOperationException(fieldType + " is not supported now.");
                case DECIMAL:
                    DecimalType decimalType = (DecimalType) fieldType;
                    if (DecimalDataUtils.is32BitDecimal(decimalType.getPrecision())) {
                        checkArgument(
                                (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                                        || typeName == PrimitiveType.PrimitiveTypeName.INT32)
                                        && primitiveType.getOriginalType() == OriginalType.DECIMAL,
                                "Unexpected type: %s", typeName);
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
                        NativeLongVector decimal64Vector = new NativeLongVector(batchSize, typeSize);
                        bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                        decimal64Vector.setPtr(bufferPtr, nullPtr, batchSize);
                        columns[i] = decimal64Vector;
                    } else {
                        checkArgument(
                                (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                                        || typeName == PrimitiveType.PrimitiveTypeName.BINARY)
                                        && primitiveType.getOriginalType() == OriginalType.DECIMAL,
                                "Unexpected type: %s", typeName);
                        NativeBytesVector decimalVector = new NativeBytesVector(batchSize, typeSize);
                        bufferPtr = Platform.allocateMemory(batchSize * typeSize);
                        decimalVector.setPtr(bufferPtr, nullPtr, batchSize);
                        columns[i] = decimalVector;
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(fieldType + " is not supported now.");
            }

            bufferPtrs[i] = bufferPtr;
        }
        return columns;
    }

    // todo: impl
    public int getRowsRead() {
        return rowRead;
    };

    public void close() throws IOException {

        if (reader != 0) {
            ParquetReaderJNI.close(reader);
        }
    }

    public boolean nextBatch() {
        if (reader == 0) {
            return false;
        }
        int rowsRead = ParquetReaderJNI.readBatch(reader, batchSize, bufferPtrs, nullPtrs);
        if (rowsRead <= 0) {
            return false;
        }
        return true;
    }

}
