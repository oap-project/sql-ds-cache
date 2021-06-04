# Advanced Configuration

In addition to usage information provided in [User Guide](User-Guide.md), we provide more strategies for SQL Index and Data Source Cache in this section.

## Cache Hot Tables

Data Source Cache also supports caching specific tables by configuring items according to actual situations, these tables are usually hot tables.

To enable caching specific hot tables, you can add the configuration below to `spark-defaults.conf`.
```
# enable table lists fiberCache
spark.sql.oap.cache.table.list.enabled          true
# Table lists using fiberCache actively
spark.sql.oap.cache.table.list                  <databasename>.<tablename1>;<databasename>.<tablename2>
```

## Column Vector Cache

This document above use **binary** cache for Parquet as example, cause binary cache can improve cache space utilization compared to ColumnVector cache. When your cluster memory resources are abundant enough, you can choose ColumnVector cache to spare computation time. 

To enable ColumnVector data cache for Parquet file format, you should add the configuration below to `spark-defaults.conf`.

```
# for parquet file format, disable binary cache
spark.sql.oap.parquet.binary.cache.enabled             false
# for parquet file format, enable ColumnVector cache
spark.sql.oap.parquet.data.cache.enabled               true
```

## Large Scale and Heterogeneous Cluster Support

***NOTE:*** Only works with `external cache`

OAP influences Spark to schedule tasks according to cache locality info. This info could be of large amount in a ***large scale cluster***, and how to schedule tasks in a ***heterogeneous cluster*** (some nodes with PMem, some without) could also be challenging.

We introduce an external DB to store cache locality info. If there's no cache available, Spark will fall back to schedule respecting HDFS locality.
Currently we support [Redis](https://redis.io/) as external DB service. Please [download and launch a redis-server](https://redis.io/download) before running Spark with OAP.

Please add the following configurations to `spark-defaults.conf`.
```
spark.sql.oap.external.cache.metaDB.enabled            true
# Redis-server address
spark.sql.oap.external.cache.metaDB.address            10.1.2.12
spark.sql.oap.external.cache.metaDB.impl               org.apache.spark.sql.execution.datasources.RedisClient
```
