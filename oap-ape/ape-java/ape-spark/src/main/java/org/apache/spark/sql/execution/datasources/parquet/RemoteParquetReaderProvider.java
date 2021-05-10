package org.apache.spark.sql.execution.datasources.parquet;

import com.intel.ape.ParquetReaderJNI;
import com.intel.ape.ParquetReaderProvider;

public class RemoteParquetReaderProvider implements ParquetReaderProvider {

    @Override
    public void init(String fileName, String hdfsHost, int hdfsPort,
                     String requiredSchema, int firstRowGroupIndex,
                     int totalGroupToRead, boolean plasmaCacheEnabled,
                     boolean preBufferEnabled, boolean plasmaCacheAsync) {
    }

    @Override
    public int readBatch(int batchSize, long[] buffers, long[] nulls) {
      return 0;
    }

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public boolean skipNextRowGroup() {
        return false;
    }

    @Override
    public void close() {}

    @Override
    public void setFilterStr(String filterStr) {}

    @Override
    public void setAggStr(String aggStr) {}

    @Override
    public void setPlasmaCacheRedis(String host, int port, String password) {}

    @Override
    public boolean isNativeEnabled() {
        return ParquetReaderJNI.isNativeEnabled();
    }
}
