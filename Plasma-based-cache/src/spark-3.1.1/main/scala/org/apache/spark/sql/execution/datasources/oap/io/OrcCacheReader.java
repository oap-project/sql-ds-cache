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

package org.apache.spark.sql.execution.datasources.oap.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.*;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache;
import org.apache.spark.sql.execution.datasources.oap.filecache.VectorDataFiberId;
import org.apache.spark.sql.execution.datasources.orc.OrcColumnVector;
import org.apache.spark.sql.execution.datasources.orc.OrcColumnVectorAllocator;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OapOnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.oap.OapRuntime$;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OrcCacheReader
    implements org.apache.spark.sql.execution.datasources.RecordReader<ColumnarBatch> {
  private static final Logger LOG = LoggerFactory.getLogger(OrcCacheReader.class);

  // TODO: make this configurable.
  protected static final int CAPACITY = 4 * 1024;

  // Vectorized ORC Row Batch
  protected VectorizedRowBatch batch;

  /**
   * The column IDs of the physical ORC file schema which are required by this reader.
   * -1 means this required column doesn't exist in the ORC file.
   */
  protected int[] requestedColIds;

  // Record reader from ORC row batch.
//  protected org.apache.orc.RecordReader recordReader;

  protected StructField[] requiredFields;

  // The result columnar batch for vectorized execution by whole-stage codegen.
  protected ColumnarBatch columnarBatch;

  // Writable column vectors of the result columnar batch.
  protected WritableColumnVector[] columnVectors;

  // The wrapped ORC column vectors. It should be null if `copyToSpark` is true.
  protected org.apache.spark.sql.vectorized.ColumnVector[] orcVectorWrappers;

  // The memory mode of the columnarBatch
  protected final MemoryMode MEMORY_MODE;

  // Whether or not to copy the ORC columnar batch to Spark columnar batch.
  protected final boolean copyToSpark;

  public Configuration configuration;

  public OrcDataFileMeta meta;

  public OrcDataFile dataFile;

  public int[] requiredColumnIds;

  protected long rowsReturned = 0;

  protected long totalCountLoadedSoFar = 0;

  protected long totalRowCount = 0;

  // Equivalent to Parquet's current rowgroup
  protected int currentStripe = -1;

  protected int currentRowGroupRowsReturned = 0;

  protected ParquetDataFiberReader[] fiberReaders;

//  public OrcCacheReader(boolean useOffHeap, boolean copyToSpark) {
//    MEMORY_MODE = useOffHeap ? MemoryMode.OFF_HEAP : MemoryMode.ON_HEAP;
//    this.copyToSpark = copyToSpark;
//  }

  public OrcCacheReader(Configuration configuration,
                        OrcDataFileMeta meta,
                        OrcDataFile dataFile,
                        int[] requiredColumnIds,
                        boolean useOffHeap,
                        boolean copyToSpark) {

    this.configuration = configuration;
    this.meta = meta;
    this.dataFile = dataFile;
    this.requiredColumnIds = requiredColumnIds;

    MEMORY_MODE = useOffHeap ? MemoryMode.OFF_HEAP : MemoryMode.ON_HEAP;
    this.copyToSpark = copyToSpark;
    this.fiberReaders = new ParquetDataFiberReader[requiredColumnIds.length];
  }

  @Override
  public ColumnarBatch getCurrentValue() {
    return columnarBatch;
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    return nextBatch();
  }

  @Override
  public void close() throws IOException {
    if (columnarBatch != null) {
      columnarBatch.close();
      columnarBatch = null;
    }
//    if (recordReader != null) {
//      recordReader.close();
//      recordReader = null;
//    }
  }

  @Override
  public void initialize() throws IOException, InterruptedException {
    // nothing required
  }

  /**
   * Initialize ORC file reader and batch record reader.
   * Please note that `initBatch` is needed to be called after this.
   * This method is customized by Oap.
   */
  public void initialize(
      Path file, Configuration conf) throws IOException {
    totalRowCount = meta.numberOfRows();
  }

  /**
   * Initialize columnar batch by setting required schema and partition information.
   * With this information, this creates ColumnarBatch with the full schema.
   */
  public void initBatch(
      TypeDescription orcSchema,
      int[] requestedColIds,
      StructField[] requiredFields,
      StructType partitionSchema,
      InternalRow partitionValues) {
    batch = orcSchema.createRowBatch(CAPACITY);
    assert(!batch.selectedInUse); // `selectedInUse` should be initialized with `false`.

    this.requiredFields = requiredFields;
    this.requestedColIds = requestedColIds;
    assert(requiredFields.length == requestedColIds.length);

    StructType resultSchema = new StructType(requiredFields);
    for (StructField f : partitionSchema.fields()) {
      resultSchema = resultSchema.add(f);
    }

    if (copyToSpark) {
      if (MEMORY_MODE == MemoryMode.OFF_HEAP) {
        columnVectors = OffHeapColumnVector.allocateColumns(CAPACITY, resultSchema);
      } else {
        columnVectors = OapOnHeapColumnVector.allocateColumns(CAPACITY, resultSchema);
      }

      // Initialize the missing columns once.
      for (int i = 0; i < requiredFields.length; i++) {
        if (requestedColIds[i] == -1) {
          columnVectors[i].putNulls(0, CAPACITY);
          columnVectors[i].setIsConstant();
        }
      }

      if (partitionValues.numFields() > 0) {
        int partitionIdx = requiredFields.length;
        for (int i = 0; i < partitionValues.numFields(); i++) {
          ColumnVectorUtils.populate(columnVectors[i + partitionIdx], partitionValues, i);
          columnVectors[i + partitionIdx].setIsConstant();
        }
      }

      columnarBatch = new ColumnarBatch(columnVectors);
    } else {
      // Just wrap the ORC column vector instead of copying it to Spark column vector.
      orcVectorWrappers =
        new org.apache.spark.sql.vectorized.ColumnVector[resultSchema.length()];

      for (int i = 0; i < requiredFields.length; i++) {
        DataType dt = requiredFields[i].dataType();
        int colId = requestedColIds[i];
        // Initialize the missing columns once.
        if (colId == -1) {
          OapOnHeapColumnVector missingCol = new OapOnHeapColumnVector(CAPACITY, dt);
          missingCol.putNulls(0, CAPACITY);
          missingCol.setIsConstant();
          orcVectorWrappers[i] = missingCol;
        } else {
          orcVectorWrappers[i] = OrcColumnVectorAllocator.allocate(dt, batch.cols[colId]);
        }
      }

      if (partitionValues.numFields() > 0) {
        int partitionIdx = requiredFields.length;
        for (int i = 0; i < partitionValues.numFields(); i++) {
          DataType dt = partitionSchema.fields()[i].dataType();
          OapOnHeapColumnVector partitionCol = new OapOnHeapColumnVector(CAPACITY, dt);
          ColumnVectorUtils.populate(partitionCol, partitionValues, i);
          partitionCol.setIsConstant();
          orcVectorWrappers[partitionIdx + i] = partitionCol;
        }
      }

      columnarBatch = new ColumnarBatch(orcVectorWrappers);
    }
  }

  // Below common code is extracted by oap index from the original nextBatch method.
  // The child class IndexedOrcColumnarBatchReader will use it as well.
  protected boolean readToColumnVectors(int batchSize) {
    if (!copyToSpark) {
      for (int i = 0; i < requiredFields.length; i++) {
        if (requestedColIds[i] != -1) {
          ((OrcColumnVector) orcVectorWrappers[i]).setBatchSize(batchSize);
        }
      }
      return true;
    }

    for (WritableColumnVector vector : columnVectors) {
      vector.reset();
    }

    for (int i = 0; i < requiredFields.length; i++) {
      StructField field = requiredFields[i];
      WritableColumnVector toColumn = columnVectors[i];

      if (requestedColIds[i] >= 0) {
        ColumnVector fromColumn = batch.cols[requestedColIds[i]];

        if (fromColumn.isRepeating) {
          putRepeatingValues(batchSize, field, fromColumn, toColumn);
        } else if (fromColumn.noNulls) {
          putNonNullValues(batchSize, field, fromColumn, toColumn);
        } else {
          putValues(batchSize, field, fromColumn, toColumn);
        }
      }
    }
    return true;

  }

  /**
   * Return true if there exists more data in the next batch. If exists, prepare the next batch
   * by copying from ORC VectorizedRowBatch columns to Spark ColumnarBatch columns.
   */
  public boolean nextBatch() throws IOException {
    if (rowsReturned >= totalRowCount) {
      return false;
    }

    checkEndOfRowGroup();
    nextBatchInternal();
    return true;
  }

  protected void checkEndOfRowGroup() {
    if (rowsReturned != totalCountLoadedSoFar) {
      return;
    }
    readNextRowGroup();
  }

  protected void readNextRowGroup() {
    currentStripe += 1;
    if (currentStripe >= meta.listStripeInformation().size()) {
      throw new IndexOutOfBoundsException("over listStripeInformation's boundary.\n");
    }

    StripeInformation stripeInformation = meta.listStripeInformation().get(currentStripe);
    long loadFiberTime = 0L;
    long loadDicTime = 0L;


    int rowCount = (int)stripeInformation.getNumberOfRows();
    for (int i = 0; i < requiredColumnIds.length; ++i) {
      long start = System.nanoTime();
      VectorDataFiberId fiberId =
          new VectorDataFiberId(dataFile, requiredColumnIds[i], currentStripe);
      FiberCache fiberCache = OapRuntime$.MODULE$.getOrCreate().fiberCacheManager().get(fiberId);
      long end = System.nanoTime();
      loadFiberTime += (end - start);
      dataFile.update(requiredColumnIds[i], fiberCache);
      long start2 = System.nanoTime();
      fiberReaders[i] = ParquetDataFiberReader$.MODULE$.apply(fiberCache.getBaseOffset(),
              columnarBatch.column(i).dataType(), rowCount);
      long end2 = System.nanoTime();
      loadDicTime += (end2 - start2);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("load row group with cols = {}, loadFiberTime = {} , loadDicTime = {} ",
              columnarBatch.numCols(), loadFiberTime, loadDicTime);
    }

    totalCountLoadedSoFar += rowCount;
    currentRowGroupRowsReturned = 0;
  }

  protected void nextBatchInternal() {
    for (WritableColumnVector vector : columnVectors) {
      vector.reset();
    }
    columnarBatch.setNumRows(0);

    int num = (int) Math.min((long)CAPACITY, totalCountLoadedSoFar - rowsReturned);
    long start = System.nanoTime();

    for (int i = 0; i < fiberReaders.length; ++i) {
      if (fiberReaders[i] == null) continue;
      fiberReaders[i].readBatch(currentRowGroupRowsReturned, num, (OapOnHeapColumnVector)columnVectors[i]);
    }

    long end = System.nanoTime();
    if (LOG.isDebugEnabled()) {
      LOG.debug("load batch with cols = {}, time = {}",
              columnarBatch.numCols(), end -start);
    }
    rowsReturned += num;
    columnarBatch.setNumRows(num);
    currentRowGroupRowsReturned += num;
  }

  public static void putRepeatingValues(
      int batchSize,
      StructField field,
      ColumnVector fromColumn,
      WritableColumnVector toColumn) {
    if (fromColumn.isNull[0]) {
      toColumn.putNulls(0, batchSize);
    } else {
      DataType type = field.dataType();
      if (type instanceof BooleanType) {
        toColumn.putBooleans(0, batchSize, ((LongColumnVector)fromColumn).vector[0] == 1);
      } else if (type instanceof ByteType) {
        toColumn.putBytes(0, batchSize, (byte)((LongColumnVector)fromColumn).vector[0]);
      } else if (type instanceof ShortType) {
        toColumn.putShorts(0, batchSize, (short)((LongColumnVector)fromColumn).vector[0]);
      } else if (type instanceof IntegerType || type instanceof DateType) {
        toColumn.putInts(0, batchSize, (int)((LongColumnVector)fromColumn).vector[0]);
      } else if (type instanceof LongType) {
        toColumn.putLongs(0, batchSize, ((LongColumnVector)fromColumn).vector[0]);
      } else if (type instanceof TimestampType) {
        toColumn.putLongs(0, batchSize,
          fromTimestampColumnVector((TimestampColumnVector)fromColumn, 0));
      } else if (type instanceof FloatType) {
        toColumn.putFloats(0, batchSize, (float)((DoubleColumnVector)fromColumn).vector[0]);
      } else if (type instanceof DoubleType) {
        toColumn.putDoubles(0, batchSize, ((DoubleColumnVector)fromColumn).vector[0]);
      } else if (type instanceof StringType || type instanceof BinaryType) {
        BytesColumnVector data = (BytesColumnVector)fromColumn;
        int size = data.vector[0].length;
        toColumn.arrayData().reserve(size);
        toColumn.arrayData().appendBytes(size, data.vector[0], 0);
        for (int index = 0; index < batchSize; index++) {
          toColumn.putArray(index, 0, size);
        }
      } else if (type instanceof DecimalType) {
        DecimalType decimalType = (DecimalType)type;
        putDecimalWritables(
          toColumn,
          batchSize,
          decimalType.precision(),
          decimalType.scale(),
          ((DecimalColumnVector)fromColumn).vector[0]);
      } else {
        throw new UnsupportedOperationException("Unsupported Data Type: " + type);
      }
    }
  }

  public static void putNonNullValues(
      int batchSize,
      StructField field,
      ColumnVector fromColumn,
      WritableColumnVector toColumn) {
    DataType type = field.dataType();
    if (type instanceof BooleanType) {
      long[] data = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        toColumn.putBoolean(index, data[index] == 1);
      }
    } else if (type instanceof ByteType) {
      long[] data = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        toColumn.putByte(index, (byte)data[index]);
      }
    } else if (type instanceof ShortType) {
      long[] data = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        toColumn.putShort(index, (short)data[index]);
      }
    } else if (type instanceof IntegerType || type instanceof DateType) {
      long[] data = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        toColumn.putInt(index, (int)data[index]);
      }
    } else if (type instanceof LongType) {
      toColumn.putLongs(0, batchSize, ((LongColumnVector)fromColumn).vector, 0);
    } else if (type instanceof TimestampType) {
      TimestampColumnVector data = ((TimestampColumnVector)fromColumn);
      for (int index = 0; index < batchSize; index++) {
        toColumn.putLong(index, fromTimestampColumnVector(data, index));
      }
    } else if (type instanceof FloatType) {
      double[] data = ((DoubleColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        toColumn.putFloat(index, (float)data[index]);
      }
    } else if (type instanceof DoubleType) {
      toColumn.putDoubles(0, batchSize, ((DoubleColumnVector)fromColumn).vector, 0);
    } else if (type instanceof StringType || type instanceof BinaryType) {
      BytesColumnVector data = ((BytesColumnVector)fromColumn);
      WritableColumnVector arrayData = toColumn.arrayData();
      int totalNumBytes = 0;
      for (int index = 0; index < batchSize; index++) {
        totalNumBytes += data.length[index];
      }
      arrayData.reserve(totalNumBytes);
      for (int index = 0, pos = 0; index < batchSize; pos += data.length[index], index++) {
        arrayData.appendBytes(data.length[index], data.vector[index], data.start[index]);
        toColumn.putArray(index, pos, data.length[index]);
      }
    } else if (type instanceof DecimalType) {
      DecimalType decimalType = (DecimalType)type;
      DecimalColumnVector data = ((DecimalColumnVector)fromColumn);
      if (decimalType.precision() > Decimal.MAX_LONG_DIGITS()) {
        toColumn.arrayData().reserve(batchSize * 16);
      }
      for (int index = 0; index < batchSize; index++) {
        putDecimalWritable(
          toColumn,
          index,
          decimalType.precision(),
          decimalType.scale(),
          data.vector[index]);
      }
    } else {
      throw new UnsupportedOperationException("Unsupported Data Type: " + type);
    }
  }

  public static void putValues(
      int batchSize,
      StructField field,
      ColumnVector fromColumn,
      WritableColumnVector toColumn) {
    DataType type = field.dataType();
    if (type instanceof BooleanType) {
      long[] vector = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        if (fromColumn.isNull[index]) {
          toColumn.putNull(index);
        } else {
          toColumn.putBoolean(index, vector[index] == 1);
        }
      }
    } else if (type instanceof ByteType) {
      long[] vector = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        if (fromColumn.isNull[index]) {
          toColumn.putNull(index);
        } else {
          toColumn.putByte(index, (byte)vector[index]);
        }
      }
    } else if (type instanceof ShortType) {
      long[] vector = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        if (fromColumn.isNull[index]) {
          toColumn.putNull(index);
        } else {
          toColumn.putShort(index, (short)vector[index]);
        }
      }
    } else if (type instanceof IntegerType || type instanceof DateType) {
      long[] vector = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        if (fromColumn.isNull[index]) {
          toColumn.putNull(index);
        } else {
          toColumn.putInt(index, (int)vector[index]);
        }
      }
    } else if (type instanceof LongType) {
      long[] vector = ((LongColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        if (fromColumn.isNull[index]) {
          toColumn.putNull(index);
        } else {
          toColumn.putLong(index, vector[index]);
        }
      }
    } else if (type instanceof TimestampType) {
      TimestampColumnVector vector = ((TimestampColumnVector)fromColumn);
      for (int index = 0; index < batchSize; index++) {
        if (fromColumn.isNull[index]) {
          toColumn.putNull(index);
        } else {
          toColumn.putLong(index, fromTimestampColumnVector(vector, index));
        }
      }
    } else if (type instanceof FloatType) {
      double[] vector = ((DoubleColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        if (fromColumn.isNull[index]) {
          toColumn.putNull(index);
        } else {
          toColumn.putFloat(index, (float)vector[index]);
        }
      }
    } else if (type instanceof DoubleType) {
      double[] vector = ((DoubleColumnVector)fromColumn).vector;
      for (int index = 0; index < batchSize; index++) {
        if (fromColumn.isNull[index]) {
          toColumn.putNull(index);
        } else {
          toColumn.putDouble(index, vector[index]);
        }
      }
    } else if (type instanceof StringType || type instanceof BinaryType) {
      BytesColumnVector vector = (BytesColumnVector)fromColumn;
      WritableColumnVector arrayData = toColumn.arrayData();
      int totalNumBytes = 0;
      for (int index = 0; index < batchSize; index++) {
        totalNumBytes += vector.length[index];
      }
      arrayData.reserve(totalNumBytes);
      for (int index = 0, pos = 0; index < batchSize; pos += vector.length[index], index++) {
        if (fromColumn.isNull[index]) {
          toColumn.putNull(index);
        } else {
          arrayData.appendBytes(vector.length[index], vector.vector[index], vector.start[index]);
          toColumn.putArray(index, pos, vector.length[index]);
        }
      }
    } else if (type instanceof DecimalType) {
      DecimalType decimalType = (DecimalType)type;
      HiveDecimalWritable[] vector = ((DecimalColumnVector)fromColumn).vector;
      if (decimalType.precision() > Decimal.MAX_LONG_DIGITS()) {
        toColumn.arrayData().reserve(batchSize * 16);
      }
      for (int index = 0; index < batchSize; index++) {
        if (fromColumn.isNull[index]) {
          toColumn.putNull(index);
        } else {
          putDecimalWritable(
            toColumn,
            index,
            decimalType.precision(),
            decimalType.scale(),
            vector[index]);
        }
      }
    } else {
      throw new UnsupportedOperationException("Unsupported Data Type: " + type);
    }
  }

  /**
   * Returns the number of micros since epoch from an element of TimestampColumnVector.
   */
  private static long fromTimestampColumnVector(TimestampColumnVector vector, int index) {
    return vector.time[index] * 1000 + (vector.nanos[index] / 1000 % 1000);
  }

  /**
   * Put a `HiveDecimalWritable` to a `WritableColumnVector`.
   */
  private static void putDecimalWritable(
      WritableColumnVector toColumn,
      int index,
      int precision,
      int scale,
      HiveDecimalWritable decimalWritable) {
    HiveDecimal decimal = decimalWritable.getHiveDecimal();
    Decimal value =
      Decimal.apply(decimal.bigDecimalValue(), decimal.precision(), decimal.scale());
    value.changePrecision(precision, scale);

    if (precision <= Decimal.MAX_INT_DIGITS()) {
      toColumn.putInt(index, (int) value.toUnscaledLong());
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      toColumn.putLong(index, value.toUnscaledLong());
    } else {
      byte[] bytes = value.toJavaBigDecimal().unscaledValue().toByteArray();
      toColumn.arrayData().putBytes(index * 16, bytes.length, bytes, 0);
      toColumn.putArray(index, index * 16, bytes.length);
    }
  }

  /**
   * Put `HiveDecimalWritable`s to a `WritableColumnVector`.
   */
  private static void putDecimalWritables(
      WritableColumnVector toColumn,
      int size,
      int precision,
      int scale,
      HiveDecimalWritable decimalWritable) {
    HiveDecimal decimal = decimalWritable.getHiveDecimal();
    Decimal value =
      Decimal.apply(decimal.bigDecimalValue(), decimal.precision(), decimal.scale());
    value.changePrecision(precision, scale);

    if (precision <= Decimal.MAX_INT_DIGITS()) {
      toColumn.putInts(0, size, (int) value.toUnscaledLong());
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      toColumn.putLongs(0, size, value.toUnscaledLong());
    } else {
      byte[] bytes = value.toJavaBigDecimal().unscaledValue().toByteArray();
      toColumn.arrayData().reserve(bytes.length);
      toColumn.arrayData().putBytes(0, bytes.length, bytes, 0);
      for (int index = 0; index < size; index++) {
        toColumn.putArray(index, 0, bytes.length);
      }
    }
  }
}
