# HCFS User Guide

* [Prerequisites](#prerequisites)
* [Configurations](#configurations)

## Prerequisites

HCFS based Data Source Cache on Spark 3.1.1 requires a working Hadoop cluster with YARN and Spark. Running Spark on YARN requires a binary distribution of Spark, which is built with YARN support. The HCFS based Data Source Cache also need to install plasma and redis, please follow [OAP-Installation-Guide](OAP-Installation-Guide.md) for how to install plasma and redis.

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
spark.hadoop.fs.cachedFs.redis.host $HOST
spark.hadoop.fs.cachedFs.redis.port $PORT
```

### Configuration for HCFS cache location policy

We provide three HCFS cache location policies, you can choose the best one for you workload
* default policy
This policy the file block locations consist of cached blocks and hdfs blocks (if cached blocks are incomplete)
* cache_over_hdfs
This policy use cached block location only if all requested content is cached, otherwise use HDFS block locations
* hdfs_only
This policy will ignoring cached blocks when finding file block locations

Add the following configuration to `$SPARK_HOME/conf/spark-defaults.conf`.

```
spark.hadoop.fs.cachedFs.blockLocation.policy  default or cache_over_hdfs or hdfs_only
```

## Configuration for HCFS cache path pattern

We provide HCFS cache patterns for paths to determine wherthe path will be cached
* allowlist
The path match the pattens will be cached. An empty regexp results in matching everything.
eg. cachedFs://localhost:9000/dir/
* denylist
The path match the pattens will not be cached. An empty regexp results in no matching of deny list.
eg. io_data|io_control

Add the following configuration to `$SPARK_HOME/conf/spark-defaults.conf`.

```
spark.hadoop.fs.cachedFs.allowList.regexp  $PATTEN
spark.hadoop.fs.cachedFs.denylist.regexp  $PATTERN
```
