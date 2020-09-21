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
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.Type;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.cacheUtil.*;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class CachedVectorizedParquetRecordReader extends SpecificParquetRecordReaderBase<Object> {

  /**
   * For each cached column, the reader to read this column from cache. This is NULL if this column
   * missing from cache, will populate the attribute with NULL.
   */
  private VectorizedCacheReader[] cacheReaders;

  /**
   * Flags for which column have been cached.
   * */
  private boolean[] cachedColumns;

  private CacheManager cacheManager;

  private FiberCache[] fiberCaches;

  /**
   * A key for each column chunk.
   */
  private ObjectId[] ids;

  StructType batchSchema;

  // The capacity of vectorized batch.
  private int capacity;

  /**
   * Batch of rows that we assemble and the current index we've returned. Every time this
   * batch is used up (batchIdx == numBatched), we populated the batch.
   */
  private int batchIdx = 0;
  private int numBatched = 0;

  /**
   * For each request column, the reader to read this column. This is NULL if this column
   * is missing from the file, in which case we populate the attribute with NULL.
   */
  private VectorizedColumnReader[] columnReaders;

  /**
   * The number of rows that have been returned.
   */
  private long rowsReturned;

  /**
   * The number of rows that have been reading, including the current in flight row group.
   */
  private long totalCountLoadedSoFar = 0;

  /**
   * For each column, true if the column is missing in the file and we'll instead return NULLs.
   */
  private boolean[] missingColumns;

  /**
   * The timezone that timestamp INT96 values should be converted to. Null if no conversion. Here to
   * workaround incompatibilities between different engines when writing timestamp values.
   */
  private TimeZone convertTz = null;

  /**
   * columnBatch object that is used for batch decoding. This is created on first use and triggers
   * batched decoding. It is not valid to interleave calls to the batched interface with the row
   * by row RecordReader APIs.
   * This is only enabled with additional flags for development. This is still a work in progress
   * and currently unsupported cases will fail with potentially difficult to diagnose errors.
   * This should be only turned on for development to work on this feature.
   * <p>
   * When this is set, the code will branch early on in the RecordReader APIs. There is no shared
   * code between the path that uses the MR decoders and the vectorized ones.
   * <p>
   * TODOs:
   * - Implement v2 page formats (just make sure we create the correct decoders).
   */
  private ColumnarBatch columnarBatch;

  private ColumnVector[] columnVectors;

  /**
   * If true, this class returns batches instead of rows.
   */
  private boolean returnColumnarBatch;

  /**
   * The memory mode of the columnarBatch
   */
  private final MemoryMode MEMORY_MODE;

  public CachedVectorizedParquetRecordReader(TimeZone convertTz, boolean useOffHeap,
                                             int capacity, SQLConf sqlConf) {
    this.convertTz = convertTz;
    MEMORY_MODE = useOffHeap ? MemoryMode.OFF_HEAP : MemoryMode.ON_HEAP;
    this.capacity = capacity;
    cacheManager = CacheManagerFactory.getOrCreate(sqlConf);
    System.err.println("Using CachedVectorizedParquetRecordReader");
  }

  /**
   * Implementation of RecordReader API.
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException, UnsupportedOperationException {
    super.initialize(inputSplit, taskAttemptContext);
    initializeInternal();
  }

  /**
   * Utility API that will read all the data in path. This circumvents the need to create Hadoop
   * objects to use this class. `columns` can contain the list of columns to project.
   */
  @Override
  public void initialize(String path, List<String> columns) throws IOException,
          UnsupportedOperationException {
    super.initialize(path, columns);
    initializeInternal();
  }

  @Override
  public void close() throws IOException {
    if (columnarBatch != null) {
      columnarBatch.close();
      columnarBatch = null;
    }
    super.close();
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    resultBatch();

    if (returnColumnarBatch) return nextBatch();

    if (batchIdx >= numBatched) {
      if (!nextBatch()) return false;
    }
    ++batchIdx;
    return true;
  }

  @Override
  public Object getCurrentValue() {
    if (returnColumnarBatch) return columnarBatch;
    return columnarBatch.getRow(batchIdx - 1);
  }

  @Override
  public float getProgress() {
    return (float) rowsReturned / totalRowCount;
  }

  // Creates a columnar batch that includes the schema from the data files and the additional
  // partition columns appended to the end of the batch.
  // For example, if the data contains two columns, with 2 partition columns:
  // Columns 0,1: data columns
  // Column 2: partitionValues[0]
  // Column 3: partitionValues[1]
  private void initBatch(
          MemoryMode memMode,
          StructType partitionColumns,
          InternalRow partitionValues) {
    batchSchema = new StructType();
    for (StructField f : sparkSchema.fields()) {
      batchSchema = batchSchema.add(f);
    }
    if (partitionColumns != null) {
      for (StructField f : partitionColumns.fields()) {
        batchSchema = batchSchema.add(f);
      }
    }

    if (memMode == MemoryMode.OFF_HEAP) {
      columnVectors = OffHeapColumnVector.allocateColumns(capacity, batchSchema);
    } else {
      columnVectors = OnHeapColumnVector.allocateColumns(capacity, batchSchema);
    }
    columnarBatch = new ColumnarBatch(columnVectors);
    if (partitionColumns != null) {
      int partitionIdx = sparkSchema.fields().length;
      for (int i = 0; i < partitionColumns.fields().length; i++) {
        ColumnVectorUtils.populate((WritableColumnVector) columnVectors[i + partitionIdx],
                partitionValues, i);
        ((WritableColumnVector)columnVectors[i + partitionIdx]).setIsConstant();
      }
    }

    // Initialize missing columns with nulls.
    for (int i = 0; i < missingColumns.length; i++) {
      if (missingColumns[i]) {
        ((WritableColumnVector)columnVectors[i]).putNulls(0, capacity);
        ((WritableColumnVector)columnVectors[i]).setIsConstant();
      }
    }
  }

  private void initBatch() {
    initBatch(MEMORY_MODE, null, null);
  }

  public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
    initBatch(MEMORY_MODE, partitionColumns, partitionValues);
  }

  /**
   * Returns the ColumnarBatch object that will be used for all rows returned by this reader.
   * This object is reused. Calling this enables the vectorized reader. This should be called
   * before any calls to nextKeyValue/nextBatch.
   */
  public ColumnarBatch resultBatch() {
    if (columnarBatch == null) initBatch();
    return columnarBatch;
  }

  /**
   * Can be called before any rows are returned to enable returning columnar batches directly.
   */
  public void enableReturningBatches() {
    returnColumnarBatch = true;
  }

  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   */
  public boolean nextBatch() throws IOException {
    columnarBatch.setNumRows(0);
    if (rowsReturned >= totalRowCount) return false;
    // we should init all readers here
    checkEndOfRowGroup();

    int num = (int) Math.min((long) capacity, totalCountLoadedSoFar - rowsReturned);

    for(int i = 0; i < columnReaders.length; ++i) {
      if(missingColumns[i]) continue;
      if(cachedColumns[i]) {
        columnVectors[i] = cacheReaders[i].readBatch(num);
      } else {
        ((WritableColumnVector)columnVectors[i]).reset();
        columnReaders[i].readBatch(num, (WritableColumnVector)columnVectors[i]);
        // TODO: async cache
        CacheDumper.syncDumpToCache((WritableColumnVector)columnVectors[i],
                (OapFiberCache) fiberCaches[i], num);
      }
    }

    rowsReturned += num;
    columnarBatch.setNumRows(num);
    numBatched = num;
    batchIdx = 0;
    return true;
  }

  private void initializeInternal() throws IOException, UnsupportedOperationException {
    // Check that the requested schema is supported.
    missingColumns = new boolean[requestedSchema.getFieldCount()];
    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    List<String[]> paths = requestedSchema.getPaths();
    for (int i = 0; i < requestedSchema.getFieldCount(); ++i) {
      Type t = requestedSchema.getFields().get(i);
      if (!t.isPrimitive() || t.isRepetition(Type.Repetition.REPEATED)) {
        throw new UnsupportedOperationException("Complex types not supported.");
      }

      String[] colPath = paths.get(i);
      if (fileSchema.containsPath(colPath)) {
        ColumnDescriptor fd = fileSchema.getColumnDescription(colPath);
        if (!fd.equals(columns.get(i))) {
          throw new UnsupportedOperationException("Schema evolution not supported.");
        }
        missingColumns[i] = false;
      } else {
        if (columns.get(i).getMaxDefinitionLevel() == 0) {
          // Column is missing in data but the required data is non-nullable. This file is invalid.
          throw new IOException("Required column is missing in data file. Col: " +
                  Arrays.toString(colPath));
        }
        missingColumns[i] = true;
      }
    }
  }

  private void checkEndOfRowGroup() throws IOException {
    if (rowsReturned != totalCountLoadedSoFar) return;
    // TODO: we need to close last cache
    for (int i = 0; i < ids.length; i++) {
      if(missingColumns[i]) continue;
      if(cachedColumns[i]) {
        // these columns are cached columns, need to release them.
        cacheManager.release(ids[i]);
      } else if (ids[i] != null) {
        // these columns are caching columns, need to seal them.
        cacheManager.seal(ids[i]);
      }
    }
    PageReadStore pages = reader.readNextRowGroup();
    if (pages == null) {
      throw new IOException("expecting more rows but reached last block. Read "
              + rowsReturned + " out of " + totalRowCount);
    }
    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    List<Type> types = requestedSchema.asGroupType().getFields();
    int columnNum = columns.size();
    columnReaders = new VectorizedColumnReader[columnNum];
    cacheReaders = new VectorizedCacheReader[columnNum];
    fiberCaches = new FiberCache[columnNum];
    cachedColumns = new boolean[columnNum];
    ids = new ObjectId[columnNum];

    for (int i = 0; i < columns.size(); ++i) {
      if (missingColumns[i]) continue;
      String key = "file + row Group Id + column Id";
      ObjectId id = new ObjectId(key);
      ids[i] = id;
      boolean hit = cacheManager.contains(id);
      if(hit) {
        try {
          fiberCaches[i] = cacheManager.get(id);
          cachedColumns[i] = true;
          cacheReaders[i] = new VectorizedCacheReader(batchSchema.fields()[i].dataType(),
                  fiberCaches[i]);
        } catch (CacheManagerException e) {
          cachedColumns[i] = false;
          cacheReaders[i] = null;
          columnReaders[i] = new VectorizedColumnReader(columns.get(i),
                  types.get(i).getOriginalType(),
                  pages.getPageReader(columns.get(i)), convertTz);
        }
      } else {
        // Maybe we should call reportCache here.
        columnReaders[i] = new VectorizedColumnReader(columns.get(i),
                types.get(i).getOriginalType(),
                pages.getPageReader(columns.get(i)), convertTz);
        boolean needCache = CacheDumper.canCache(batchSchema.fields()[i].dataType());
        if(needCache) {
          long length = CacheDumper.calculateLength(batchSchema.fields()[i].dataType(),
                                                    pages.getRowCount());
          fiberCaches[i] = cacheManager.create(id, length);
        } else {
          // this columns can NOT cache.
          ids[i] = null;
        }

      }
    }
    totalCountLoadedSoFar += pages.getRowCount();
  }
}
