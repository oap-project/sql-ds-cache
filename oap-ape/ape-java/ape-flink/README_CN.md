# APE-FLINK 介绍
(英文版在此: [English version](README.md))

APE项目提供了一系列对Spark SQL和Flink SQL的优化方案，包括:

* HDFS Parquet文件的C++ Reader
* Filter下推
* Aggregate下推
* Parquet Column Chunk缓存
* 远程模式的APE服务

本文将介绍如何使用APE的上述优化。同时也将列举其他几个优化方案，这些方案均是利用[Intel Persistent Memory](https://www.intel.com/content/www/us/en/architecture-and-technology/optane-dc-persistent-memory.html)来优化Flink性能。


## 目录
<!-- MarkdownTOC autolink="true" autoanchor="true" -->

- [一、Flink批处理模式优化: APE SQL](#%E4%B8%80%E3%80%81flink%E6%89%B9%E5%A4%84%E7%90%86%E6%A8%A1%E5%BC%8F%E4%BC%98%E5%8C%96-ape-sql)
    - [APE软件依赖的安装步骤](#ape%E8%BD%AF%E4%BB%B6%E4%BE%9D%E8%B5%96%E7%9A%84%E5%AE%89%E8%A3%85%E6%AD%A5%E9%AA%A4)
    - [APE-Server安装步骤 \(remote模式需要\)](#ape-server%E5%AE%89%E8%A3%85%E6%AD%A5%E9%AA%A4-remote%E6%A8%A1%E5%BC%8F%E9%9C%80%E8%A6%81)
    - [基于APE开发Flink应用程序](#%E5%9F%BA%E4%BA%8Eape%E5%BC%80%E5%8F%91flink%E5%BA%94%E7%94%A8%E7%A8%8B%E5%BA%8F)
- [二、其他利用PMem优化Flink性能方案](#%E4%BA%8C%E3%80%81%E5%85%B6%E4%BB%96%E5%88%A9%E7%94%A8pmem%E4%BC%98%E5%8C%96flink%E6%80%A7%E8%83%BD%E6%96%B9%E6%A1%88)
    - [1. Flink实时流模式优化: RocksDB on PMem](#1-flink%E5%AE%9E%E6%97%B6%E6%B5%81%E6%A8%A1%E5%BC%8F%E4%BC%98%E5%8C%96-rocksdb-on-pmem)
    - [2. Flink批处理模式优化: Spill on PMem](#2-flink%E6%89%B9%E5%A4%84%E7%90%86%E6%A8%A1%E5%BC%8F%E4%BC%98%E5%8C%96-spill-on-pmem)
    - [3. Flink批处理模式优化: HDFS文件系统缓存](#3-flink%E6%89%B9%E5%A4%84%E7%90%86%E6%A8%A1%E5%BC%8F%E4%BC%98%E5%8C%96-hdfs%E6%96%87%E4%BB%B6%E7%B3%BB%E7%BB%9F%E7%BC%93%E5%AD%98)

<!-- /MarkdownTOC -->

<a id="%E4%B8%80%E3%80%81flink%E6%89%B9%E5%A4%84%E7%90%86%E6%A8%A1%E5%BC%8F%E4%BC%98%E5%8C%96-ape-sql"></a>
## 一、Flink批处理模式优化: APE SQL

首先需要注意的是，APE项目的SQL优化都是针对`HDFS`中的`parquet`文件格式进行的，元数据保存于`hive`中。

APE优化库支持2种集成方式：可以作为library运行于每一个Flink TaskManager之下(`local模式`)，也可以单独部署以远程方式访问(`remote模式`)。

local模式下，APE的软件依赖需要预先安装到Flink运行的集群中，例如`Yarn`集群。

remote模式下，Flink运行集群不需要安装APE的软件依赖。APE的软件依赖需要安装到独立的一个集群中，并且启动`APE-Server`。


<a id="ape%E8%BD%AF%E4%BB%B6%E4%BE%9D%E8%B5%96%E7%9A%84%E5%AE%89%E8%A3%85%E6%AD%A5%E9%AA%A4"></a>
### APE软件依赖的安装步骤

- **arrow**
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

- **Intel Codec Library**
```
git clone https://github.com/Intel-bigdata/IntelCodecLibrary.git
cd IntelCodecLibrary


mvn clean install -DskipTests

```

- **APE C++库**
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

<a id="ape-server%E5%AE%89%E8%A3%85%E6%AD%A5%E9%AA%A4-remote%E6%A8%A1%E5%BC%8F%E9%9C%80%E8%A6%81"></a>
### APE-Server安装步骤 (remote模式需要)

(注意: APE-Server运行依赖上述软件的安装)

- **安装运行APE-Server**
```
git clone https://github.com/oap-project/sql-ds-cache.git
cd sql-ds-cache
git checkout -b ape origin/ape


cd oap-ape/ape-java/
mvn clean install
mvn clean install -Pshading -pl ape-server

cd ape-server
java -cp target/*:$HADOOP_CLASSPATH com.intel.oap.ape.service.netty.server.NettyServer

```

(需要记录下每一个APE-Server进程所在的节点`hostname`和监听端口`port`，客户端会用到)


<a id="%E5%9F%BA%E4%BA%8Eape%E5%BC%80%E5%8F%91flink%E5%BA%94%E7%94%A8%E7%A8%8B%E5%BA%8F"></a>
### 基于APE开发Flink应用程序

(注意: 开发Flink应用不依赖上述软件的安装)

- **在开发环境安装APE Java库**
```
git clone https://github.com/oap-project/pmem-common.git
cd pmem-common
git checkout branch-1.1-spark-3.x


mvn clean install -DskipTests


git clone https://github.com/oap-project/sql-ds-cache.git
cd sql-ds-cache
git checkout -b ape origin/ape


cd oap-ape/ape-java/
mvn clean install
mvn clean package -Pshading -pl :ape-flink

cp ape-flink/target/ape-flink-1.1.0-SNAPSHOT.jar $FLINK_INSTALL_DIR/lib/

```


- **在Flink应用中激活APE优化选项**

(注意: 开启C++ Reader是激活其他优化的前提)

```
    // 激活APE优化的基本要求是在Flink应用中使用APE提供的`TableEnvrionment`和`HiveCatalog`
    // Create APE's hive table env
    ApeEnvironmentSettings settings = ApeEnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnv = ApeTableEnvironmentFactory.create(settings);

    // Set APE's HiveCatalog as the catalog of current session
    Catalog catalog = new ApeHiveCatalog(hiveCatalogName, hiveDefaultDBName, hiveConfDir);
    tableEnv.registerCatalog(hiveCatalogName, catalog);
    tableEnv.useCatalog(hiveCatalogName);

    // Use hive dialect
    tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

    // 激活优化选项的方式如下
    // HDFS Parquet文件的C++ Reader
    tableEnv.getConfig().getConfiguration().setBoolean(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_USE_NATIVE_READER.key(), true);
    // local模式的APE服务
    tableEnv.getConfig().getConfiguration().setString(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_NATIVE_READER_MODE.key(), "local");
    // remote模式的APE服务
    tableEnv.getConfig().getConfiguration().setString(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_NATIVE_READER_MODE.key(), "remote");
    // Filter下推
    tableEnv.getConfig().getConfiguration().setBoolean(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_PUSH_DOWN_FILTERS.key(), true);
    // Aggregate下推
    tableEnv.getConfig().getConfiguration().setBoolean(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_PUSH_DOWN_AGGREGATIONS.key(), true);
```

- **激活优化: Parquet Column Chunk缓存 的附加步骤**

(注意: 开启此优化需要在Flink运行集群(local模式)或APE-server节点(remote模式)上，启动arrow的`plasma-store-server`用于缓存管理，参考: [plasma](https://arrow.apache.org/docs/python/plasma.html)。

`socket`参数需配置为: `-s /tmp/plasmaStore`。)


在提交Flink应用的节点上配置hadoop的hdfs-site.xml:
```
    <property>
        <name>fs.ape.reader.plasmaCacheEnabled</name>
        <value>true</value>
    </property>

```

- **激活优化: remote模式的APE服务 的附加步骤**

在提交Flink应用的节点上配置hadoop的hdfs-site.xml:
```
    <property>
        <name>fs.ape.client.remote.servers</name>
        <value>host1:port1,host2:port2</value>
    </property>

```

- **提交Flink应用前的准备**

```
cp oap-ape/ape-java/ape-flink/target/ape-flink-1.1.0-SNAPSHOT.jar $FLINK_HOME/lib
```


<a id="%E4%BA%8C%E3%80%81%E5%85%B6%E4%BB%96%E5%88%A9%E7%94%A8pmem%E4%BC%98%E5%8C%96flink%E6%80%A7%E8%83%BD%E6%96%B9%E6%A1%88"></a>
## 二、其他利用PMem优化Flink性能方案


<a id="1-flink%E5%AE%9E%E6%97%B6%E6%B5%81%E6%A8%A1%E5%BC%8F%E4%BC%98%E5%8C%96-rocksdb-on-pmem"></a>
### 1. Flink实时流模式优化: RocksDB on PMem

此优化方案中，将Intel Persitent Memory以SoAD模式运行，作为Flink中RocksDB的存储媒介。

- **激活方式**

修改Flink客户端配置文件flink-conf.yaml:
```
state.backend.rocksdb.localdir: /mnt/pmem

```


<a id="2-flink%E6%89%B9%E5%A4%84%E7%90%86%E6%A8%A1%E5%BC%8F%E4%BC%98%E5%8C%96-spill-on-pmem"></a>
### 2. Flink批处理模式优化: Spill on PMem

此优化方案中，将Intel Persitent Memory以SoAD模式运行，作为Flink中部分临时数据的存储媒介。

- **一般激活方式:**

修改Flink客户端配置文件flink-conf.yaml:
```
io.tmp.dirs: /mnt/pmem

```

- **NUMA binding激活方式:**

首先，安装APE-Flink Java库
```
cp oap-ape/ape-java/ape-flink/target/ape-flink-1.1.0-SNAPSHOT.jar $FLINK_HOME/lib
```

然后，修改Flink客户端配置文件flink-conf.yaml:
```
    yarn.container-start-command-template: %java% %jvmmem% %jvmopts% %logging% %class%Ape %args% %redirects%

    yarn.container-numa-binding.node-list: 0;1

    yarn.container-numa-binding.path-list: /mnt/pmem0;/mnt/pmem1

    shuffle-service-factory.class: org.apache.flink.runtime.io.network.ApeNettyShuffleServiceFactory

```


<a id="3-flink%E6%89%B9%E5%A4%84%E7%90%86%E6%A8%A1%E5%BC%8F%E4%BC%98%E5%8C%96-hdfs%E6%96%87%E4%BB%B6%E7%B3%BB%E7%BB%9F%E7%BC%93%E5%AD%98"></a>
### 3. Flink批处理模式优化: HDFS文件系统缓存

此优化方案中，将Intel Persitent Memory以AD模式运行，作为HDFS文件在Flink运行集群节点本地缓存的存储媒介。

提供一个兼容HDFS文件操作接口的协议`cachedFs`，可以如同访问普通HDFS文件一样来访问将被缓存的文件。

(

注意: 

开启此优化需要在Flink运行集群启动arrow的`plasma-store-server`用于缓存管理，参考: [plasma](https://arrow.apache.org/docs/python/plasma.html)。`socket`参数需配置为: `-s /tmp/plasmaStore`。

此外，还需要一个redis服务，用于存储缓存元数据。

)


- **安装HCFS Java库到Flink运行集群**
```
git clone https://github.com/oap-project/sql-ds-cache
cd sql-ds-cache/HCFS-based-cache

mvn clean package

cp target/hcfs-sql-ds-cache-1.1.0.jar $HADOOP_HOME/shared/hadoop/hdfs/lib
```

- **Hive中创建表时基于`cachedFs`协议头，例如:**
```
CREATE EXTERNAL TABLE IF NOT EXISTS `customer` (
  `c_custkey` BIGINT,
  `c_name` STRING,
  `c_address` STRING,
  `c_nationkey` BIGINT,
  `c_phone` STRING,
  `c_acctbal` DECIMAL(12,2),
  `c_mktsegment` STRING,
  `c_comment` STRING)
STORED AS PARQUET TBLPROPERTIES ('parquet.compression'='SNAPPY')
LOCATION 'cachedFs://localhost:9000/tpch_1t_snappy/customer';

```


- **在提交Flink应用的节点上配置hadoop的hdfs-site.xml:**
```
    <property>
        <name>fs.cachedFs.impl</name>
        <value>com.intel.oap.fs.hadoop.cachedfs.CachedFileSystem</value>
    </property>
    <property>
        <name>fs.cachedFs.redis.host</name>
        <value>localhost</value>
    </property>
    <property>
        <name>fs.cachedFs.redis.port</name>
        <value>6379</value>
    </property>

```
