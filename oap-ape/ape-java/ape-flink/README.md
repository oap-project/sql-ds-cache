# APE-FLINK
APE engine is designed to load parquet files from HDFS clusters with native reader for faster data loading with cache capability, filters, and aggregate functions.

This module bridges customized Flink and native APE engine.


## Table of Contents
<!-- MarkdownTOC autolink="true" autoanchor="true" -->

- [Install](#install)
    - [1. Install Apache Hadoop and Apache Hive for data and resource management.](#1-install-apache-hadoop-and-apache-hive-for-data-and-resource-management)
    - [2. Build and install arrow](#2-build-and-install-arrow)
    - [3. Build and install APE-native](#3-build-and-install-ape-native)
    - [4. Install Apache Flink](#4-install-apache-flink)
    - [5. Build pmem-common](#5-build-pmem-common)
    - [6. Build APE-java](#6-build-ape-java)
    - [7. Build Flink with our patch](#7-build-flink-with-our-patch)
- [Usages](#usages)
    - [Enable the native parquet reader provided by APE](#enable-the-native-parquet-reader-provided-by-ape)
    - [Enable filters pushing down](#enable-filters-pushing-down)
    - [Enable aggregates pushing down \(experimental\)](#enable-aggregates-pushing-down-experimental)
    - [Enable cache of column chunks in native parquet reader](#enable-cache-of-column-chunks-in-native-parquet-reader)
    - [APE in disaggregated mode](#ape-in-disaggregated-mode)

<!-- /MarkdownTOC -->


<a id="install"></a>
## Install
<a id="1-install-apache-hadoop-and-apache-hive-for-data-and-resource-management"></a>
### 1. Install [Apache Hadoop](https://hadoop.apache.org/releases.html) and [Apache Hive](https://hive.apache.org/downloads.html) for data and resource management.
***(on your cluster nodes)***


<a id="2-build-and-install-arrow"></a>
### 2. Build and install arrow
***(on each worker of Hadoop Yarn)***

```
git clone https://github.com/oap-project/arrow.git
cd arrow
git checkout -b ape-dev origin/ape-dev

ARROW_INSTALL_DIR=/usr
rm -rf cpp/release-build
mkdir -p cpp/release-build
cd cpp/release-build
cmake -DCMAKE_INSTALL_PREFIX=$ARROW_INSTALL_DIR -DARROW_PARQUET=ON -DARROW_HDFS=ON -DARROW_JNI=ON -DARROW_FILESYSTEM=ON -DARROW_ORC=ON -DARROW_PLASMA_JAVA_CLIENT=on -DARROW_PLASMA=on -DARROW_DEPENDENCY_SOURCE=BUNDLED ..
make -j

sudo make install
```

<a id="3-build-and-install-ape-native"></a>
### 3. Build and install APE-native
***(on each worker of Hadoop Yarn)***

```
git clone https://github.com/oap-project/sql-ds-cache.git
cd sql-ds-cache
git checkout -b ape origin/ape


mkdir -p oap-ape/ape-native/build
cd oap-ape/ape-native/build
cmake ..
make

sudo cp oap-ape/ape-native/build/lib/libparquet_jni.so /usr/lib/
```


<a id="4-install-apache-flink"></a>
### 4. Install [Apache Flink](https://flink.apache.org/downloads.html)
***(on client node, we call it `client-node` below)***

`FLINK_INSTALL_DIR` is assumed as the Flink install directory in below steps.

To access data in Hive, you may need:
* develop applications following [this guide](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/hive/)
* copy a jar file named `hadoop-mapreduce-client-core-x.x.x.jar` from `$HADOOP_HOME/share/hadoop/mapreduce/` to `$FLINK_INSTALL_DIR/lib/`
* and set `source.reader.element.queue.capacity: 1` in `$FLINK_INSTALL_DIR/conf/flink-conf/yaml` besource exceptions may come out when Flink decoding parquet data


<a id="5-build-pmem-common"></a>
### 5. Build pmem-common
***(on `client-node`)***

```
git clone https://github.com/oap-project/pmem-common.git
cd pmem-common


mvn clean install -DskipTests

```


<a id="6-build-ape-java"></a>
### 6. Build APE-java
***(on `client-node`)***

```
git clone https://github.com/oap-project/sql-ds-cache.git
cd sql-ds-cache
git checkout -b ape origin/ape


cd oap-ape/ape-java/
mvn clean install
mvn clean package -Pshading -pl :ape-flink

cp ape-flink/target/ape-flink-1.1.0-SNAPSHOT.jar $FLINK_INSTALL_DIR/lib/
```

<a id="7-build-flink-with-our-patch"></a>
### 7. Build Flink with our patch
***(on `client-node`)***

`SQL_DS_CACHE_SOURCE_DIR` is assumed as the source code directory of this project in below steps.
Patches are under `src/main/resources/flink-patches` of this module.

```
# for Flink version 1.12.0
git clone https://github.com/apache/flink.git
cd flink
git checkout release-1.12.0
git am $SQL_DS_CACHE_SOURCE_DIR/oap-ape/ape-java/ape-flink/src/main/resources/flink-patches/1.12.0/ape-patch-for-flink-1.12.0.patch
mvn clean install -pl :flink-sql-connector-hive-2.3.6_2.11 -pl :flink-table-uber_2.11 -pl :flink-table-uber-blink_2.11 -DskipTests


cp flink-connectors/flink-sql-connector-hive-2.3.6/target/flink-sql-connector-hive-2.3.6_2.11-1.12.0.jar $FLINK_INSTALL_DIR/lib/
cp flink-table/flink-table-uber/target/flink-table-uber_2.11-1.12.0.jar $FLINK_INSTALL_DIR/lib/flink-table_2.11-1.12.0.jar
cp flink-table/flink-table-uber-blink/target/flink-table-uber-blink_2.11-1.12.0.jar $FLINK_INSTALL_DIR/lib/flink-table-blink_2.11-1.12.0.jar

```

<a id="usages"></a>
## Usages
<a id="enable-the-native-parquet-reader-provided-by-ape"></a>
### Enable the native parquet reader provided by APE
***(config in applications)***

```
tableEnv.getConfig().getConfiguration().setBoolean(HiveOptions.TABLE_EXEC_HIVE_PARQUET_USE_NATIVE_READER.key(), true);
```


<a id="enable-filters-pushing-down"></a>
### Enable filters pushing down
***(config in Flink applications)***

```
tableEnv.getConfig().getConfiguration().setBoolean(HiveOptions.TABLE_EXEC_HIVE_PARQUET_USE_NATIVE_READER.key(), true);
tableEnv.getConfig().getConfiguration().setBoolean(HiveOptions.TABLE_EXEC_HIVE_PARQUET_PUSH_DOWN_FILTERS.key(), true);
```


<a id="enable-aggregates-pushing-down-experimental"></a>
### Enable aggregates pushing down (experimental)
***(config in Flink applications)***

```
tableEnv.getConfig().getConfiguration().setBoolean(HiveOptions.TABLE_EXEC_HIVE_PARQUET_USE_NATIVE_READER.key(), true);
tableEnv.getConfig().getConfiguration().setBoolean(HiveOptions.TABLE_EXEC_HIVE_PARQUET_PUSH_DOWN_FILTERS.key(), true);
tableEnv.getConfig().getConfiguration().setBoolean(HiveOptions.TABLE_EXEC_HIVE_PARQUET_PUSH_DOWN_AGGREGATIONS.key(), true);
```


<a id="enable-cache-of-column-chunks-in-native-parquet-reader"></a>
### Enable cache of column chunks in native parquet reader
***(config in Flink applications)***

```
tableEnv.getConfig().getConfiguration().setBoolean(HiveOptions.TABLE_EXEC_HIVE_PARQUET_USE_NATIVE_READER.key(), true);
```

***(config in $HADOOP_HOME/etc/hadoop/hdfs-site.xml on `client-node`)***

Required Configurations:
```
    <property>
        <name>fs.ape.reader.plasmaCacheEnabled</name>
        <value>true</value>
    </property>
```

Optional configurations for cache locality:

```
<!-- save cache location to redis -->
    <property>
        <name>fs.ape.reader.cacheLocalityEnabled</name>
        <value>true</value>
    </property>

<!-- redis host to save cache location -->
    <property>
        <name>fs.ape.hcfs.redis.host</name>
        <value>sr490</value>
    </property>

<!-- redis port to save cache location -->
    <property>
        <name>fs.ape.hcfs.redis.port</name>
        <value>6379</value>
    </property>

<!-- use cache aware FileSystem implementation -->
    <property>
        <name>fs.hdfs.impl</name>
        <value>com.intel.oap.fs.hadoop.ape.hcfs.CacheAwareFileSystem</value>
    </property>

<!-- policy when using cache location info -->
    <property>
        <name>fs.ape.hcfs.blockLocation.policy</name>
        <value>hdfs_only</value>
    </property>

```

<a id="ape-in-disaggregated-mode"></a>
### APE in disaggregated mode
With below configurations, Flink can request data from remote APE servers in separate clusters.

In this mode, `arrow` and `ape-native` are not required on Hadoop Yarn nodes.

***(config in Flink applications)***

```
tableEnv.getConfig().getConfiguration().setString(HiveOptions.TABLE_EXEC_HIVE_PARQUET_NATIVE_READER_MODE.key(), "remote");
```

***(config in $HADOOP_HOME/etc/hadoop/hdfs-site.xml on `client-node`)***

```
    <property>
        <name>fs.ape.client.remote.servers</name>
        <value>host1:port1,host2:port2</value>
    </property>
```
