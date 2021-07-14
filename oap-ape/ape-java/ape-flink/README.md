# APE-FLINK
APE engine is designed to load parquet files from HDFS clusters with native reader for faster data loading with cache capability, filters, and aggregate functions.

This module bridges customized Flink and native APE engine.


## Table of Contents
<!-- MarkdownTOC autolink="true" autoanchor="true" -->

- [Install](#install)
    - [1. Install Apache Hadoop and Apache Hive](#1-install-apache-hadoop-and-apache-hive)
    - [2. Install Apache Flink](#2-install-apache-flink)
    - [3. Build and install Arrow libraries](#3-build-and-install-arrow-libraries)
    - [4. Build and install APE native library](#4-build-and-install-ape-native-library)
    - [5. Build and install pmem-common](#5-build-and-install-pmem-common)
    - [6. Build and install Intel Codec Library](#6-build-and-install-intel-codec-library)
    - [7. Build and install APE Java library](#7-build-and-install-ape-java-library)
- [Usages](#usages)
    - [Use table environment provided by APE](#use-table-environment-provided-by-ape)
    - [Enable the native parquet reader](#enable-the-native-parquet-reader)
    - [Enable filters pushing down](#enable-filters-pushing-down)
    - [Enable aggregates pushing down \(experimental\)](#enable-aggregates-pushing-down-experimental)
    - [Enable cache of column chunks in native parquet reader](#enable-cache-of-column-chunks-in-native-parquet-reader)
    - [APE in disaggregated mode](#ape-in-disaggregated-mode)

<!-- /MarkdownTOC -->


<a id="install"></a>
## Install
<a id="1-install-apache-hadoop-and-apache-hive"></a>
### 1. Install [Apache Hadoop](https://hadoop.apache.org/releases.html) and [Apache Hive](https://hive.apache.org/downloads.html)
***(on your cluster nodes)***

<a id="2-install-apache-flink"></a>
### 2. Install [Apache Flink](https://flink.apache.org/downloads.html)
***(on client node, we call it `client-node` below)***

`FLINK_INSTALL_DIR` is assumed as the Flink install directory in below steps.

To access data in Hive, you may need:
* develop applications following [this guide](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/hive/)
* copy a jar file named `hadoop-mapreduce-client-core-x.x.x.jar` from `$HADOOP_HOME/share/hadoop/mapreduce/` to `$FLINK_INSTALL_DIR/lib/`
* and set `source.reader.element.queue.capacity: 1` in `$FLINK_INSTALL_DIR/conf/flink-conf/yaml` because exceptions may come out when Flink decoding parquet data


<a id="3-build-and-install-arrow-libraries"></a>
### 3. Build and install Arrow libraries
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

<a id="4-build-and-install-ape-native-library"></a>
### 4. Build and install APE native library
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


<a id="5-build-and-install-pmem-common"></a>
### 5. Build and install pmem-common
***(on `client-node`)***

```
git clone https://github.com/oap-project/pmem-common.git
cd pmem-common
git checkout branch-1.1-spark-3.x


mvn clean install -DskipTests

```

<a id="6-build-and-install-intel-codec-library"></a>
### 6. Build and install Intel Codec Library
***(on `client-node`)***

```
git clone https://github.com/Intel-bigdata/IntelCodecLibrary.git
cd IntelCodecLibrary


mvn clean install -DskipTests

```


<a id="7-build-and-install-ape-java-library"></a>
### 7. Build and install APE Java library
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

<a id="usages"></a>
## Usages
<a id="use-table-environment-provided-by-ape"></a>
### Use table environment provided by APE
***(config in applications, required by following features)***
```
    // Create APE's hive table env
    ApeEnvironmentSettings settings = ApeEnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = ApeTableEnvironmentFactory.create(settings);

    // Set APE's HiveCatalog as the catalog of current session
    Catalog catalog = new ApeHiveCatalog(hiveCatalogName, hiveDefaultDBName, hiveConfDir);
    tableEnv.registerCatalog(hiveCatalogName, catalog);
    tableEnv.useCatalog(hiveCatalogName);

    // Use hive dialect
    tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
```

<a id="enable-the-native-parquet-reader"></a>
### Enable the native parquet reader
***(config in applications)***

```
    tableEnv.getConfig().getConfiguration().setBoolean(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_USE_NATIVE_READER.key(), true);
```


<a id="enable-filters-pushing-down"></a>
### Enable filters pushing down
***(config in Flink applications)***

```
    tableEnv.getConfig().getConfiguration().setBoolean(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_USE_NATIVE_READER.key(), true);
    tableEnv.getConfig().getConfiguration().setBoolean(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_PUSH_DOWN_FILTERS.key(), true);
```


<a id="enable-aggregates-pushing-down-experimental"></a>
### Enable aggregates pushing down (experimental)
***(config in Flink applications)***

```
    tableEnv.getConfig().getConfiguration().setBoolean(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_USE_NATIVE_READER.key(), true);
    tableEnv.getConfig().getConfiguration().setBoolean(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_PUSH_DOWN_FILTERS.key(), true);
    tableEnv.getConfig().getConfiguration().setBoolean(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_PUSH_DOWN_AGGREGATIONS.key(), true);
```


<a id="enable-cache-of-column-chunks-in-native-parquet-reader"></a>
### Enable cache of column chunks in native parquet reader
***(config in Flink applications)***

```
    tableEnv.getConfig().getConfiguration().setBoolean(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_USE_NATIVE_READER.key(), true);
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

***(start ape servers on some nodes with `arrow` and `ape-native`)***

See more at: [../ape-server/README.md](../ape-server/README.md)


***(config in Flink applications)***

```
    tableEnv.getConfig().getConfiguration().setString(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_NATIVE_READER_MODE.key(), "remote");
```

***(config in $HADOOP_HOME/etc/hadoop/hdfs-site.xml on `client-node`)***

```
    <property>
        <name>fs.ape.client.remote.servers</name>
        <value>host1:port1,host2:port2</value>
    </property>
```
