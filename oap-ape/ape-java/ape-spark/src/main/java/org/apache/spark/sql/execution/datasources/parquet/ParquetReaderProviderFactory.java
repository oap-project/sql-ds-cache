package org.apache.spark.sql.execution.datasources.parquet;

import com.intel.ape.ParquetReaderProvider;
import com.intel.ape.ParquetReaderProviderType;

public class ParquetReaderProviderFactory {
  public static ParquetReaderProvider getParquetReaderProvider(
          ParquetReaderProviderType parquetReaderProviderType) {
    if (parquetReaderProviderType == ParquetReaderProviderType.LOCAL) {
      return new LocalParquetReaderProvider();
    } else if (parquetReaderProviderType == ParquetReaderProviderType.REMOTE) {
      return new RemoteParquetReaderProvider();
    }
    return null;
  }
}
