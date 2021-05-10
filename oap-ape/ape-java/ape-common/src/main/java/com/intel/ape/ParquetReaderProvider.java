package com.intel.ape;

public interface ParquetReaderProvider {
  // initialize parquet data reader.
  // For local, reader pointer would be returned from local parquet JNI.
  // For remote, netty client is requested and will provide data reading service
  void init(String fileName, String hdfsHost, int hdfsPort,
            String requiredSchema, int firstRowGroupIndex,
            int totalGroupToRead, boolean plasmaCacheEnabled,
            boolean preBufferEnabled, boolean plasmaCacheAsync);

  int readBatch(int batchSize, long[] buffers, long[] nulls);

  void close();

  boolean hasNext();

  boolean skipNextRowGroup();

  void setFilterStr(String filterStr);

  void setAggStr(String aggStr);

  void setPlasmaCacheRedis(String host, int port, String password);

  boolean isNativeEnabled();
}
