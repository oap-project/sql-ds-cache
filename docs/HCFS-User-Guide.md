# HCFS User Guide

* [Prerequisites](#prerequisites)
* [Configurations](#configuration)

## Prerequisites

HCFS based Data Source Cache on Spark 3.0.0 requires a working Hadoop cluster with YARN and Spark. Running Spark on YARN requires a binary distribution of Spark, which is built with YARN support. The HCFS based Data Source Cache also need to install plasma and redis, please follow [OAP-Installation-Guide](OAP-Installation-Guide.md) for how to install plasma and redis.

## Configurations

### Spark Configurations

Before you run `$SPARK_HOME/bin/spark-shell `, you need to configure Spark for integration. You need to add or update the following configurations in the Spark configuration file `$SPARK_HOME/conf/spark-defaults.conf` on your working node.

```bash
spark.hadoop.fs.cachedFs.impl com.intel.oap.fs.hadoop.cachedfs.CachedFileSystem
# absolute path of the jar on your working node
spark.files                       /path/to/hcfs-sql-ds-cache-<version>.jar
# relative path to spark.files, just specify jar name in current dir
spark.executor.extraClassPath     ./hcfs-sql-cache-<version>.jar
# absolute path of the jar on your working node
spark.driver.extraClassPath       /path/to/hcfs-sql-ds-cache-<version>.jar
```

### Redis Configuration

Add the following configuration to `$SPARK_HOME/conf/spark-defaults.conf`.

```
spark hadoop.fs.cachedFs.redis.host
spark hadoop.fs.cachedFs.redis.port
```

### Configuration for HCFS cache location policy

We provide three HCFS cache location policies, you can choose the best one for you workload
* defalut policy
This policy the file block locations consist of cached blocks and hdfs blocks (if cached blocks are incomplete)
* cache_over_hdfs
This policy use cached block location only if all requested content is cached, otherwise use HDFS block locations
* hdfs_only
This policy will ignoring cached blocks when finding file block locations

Add the following configuration to `$SPARK_HOME/conf/spark-defaults.conf`.

```
spark hadoop.fs.cachedFs.blockLocation.policy  default or cache_over_hdfs or hdfs_only
```

## Configuration for HCFS cache path patten

We provide HCFS cache patterns for paths to determine wherthe path will be cached
* whitelist
The path match the pattens will be cached. An empty regexp results in matching everything.
* blacklist
The path match the pattens will not be cached. An empty regexp results in no matching of black list.

Add the following configuration to `$SPARK_HOME/conf/spark-defaults.conf`.

```
spark.hadoop.fs.cachedFs.whiteList.regexp
spark.hadoop.fs.cachedFs.blacklist.regexp
```
