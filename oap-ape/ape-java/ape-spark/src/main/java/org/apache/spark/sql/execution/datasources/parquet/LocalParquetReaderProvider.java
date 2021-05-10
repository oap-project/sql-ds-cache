package org.apache.spark.sql.execution.datasources.parquet;

import com.intel.ape.ParquetReaderJNI;
import com.intel.ape.ParquetReaderProvider;

public class LocalParquetReaderProvider implements ParquetReaderProvider {
  private long reader;

  @Override
  public void init(String fileName, String hdfsHost, int hdfsPort,
                   String requiredSchema, int firstRowGroupIndex,
                   int totalGroupToRead, boolean plasmaCacheEnabled,
                   boolean preBufferEnabled, boolean plasmaCacheAsync) {
    reader = ParquetReaderJNI.init(fileName, hdfsHost, hdfsPort,
            requiredSchema, firstRowGroupIndex, totalGroupToRead, plasmaCacheEnabled,
            preBufferEnabled, plasmaCacheAsync);
  }

  @Override
  public int readBatch(int batchSize, long[] buffers, long[] nulls) {
    return ParquetReaderJNI.readBatch(reader,batchSize, buffers,nulls);
  }

  @Override
  public boolean hasNext() {
    return ParquetReaderJNI.hasNext(reader);
  }

  @Override
  public boolean skipNextRowGroup() {
    return ParquetReaderJNI.skipNextRowGroup(reader);
  }

  @Override
  public void close() {
    ParquetReaderJNI.close(reader);
  }
  @Override
  public void setFilterStr(String filterStr) {
    ParquetReaderJNI.setFilterStr(reader, filterStr);
  }

  @Override
  public void setAggStr(String aggStr) {
    ParquetReaderJNI.setAggStr(reader, aggStr);
  }

  @Override
  public void setPlasmaCacheRedis(String host, int port, String password) {
      ParquetReaderJNI.setPlasmaCacheRedis(reader, host, port, password);
  }

  @Override
  public boolean isNativeEnabled() {
      return ParquetReaderJNI.isNativeEnabled();
  }
}
