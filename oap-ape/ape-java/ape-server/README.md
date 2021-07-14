# APE-SERVER

`ape-server` can run as a serive provider with APE libraries in separate clusters.

This decouples Spark/Flink workers and APE service, saving resources and making it easy to manage clusters.

Also, this makes Hadoop Yarn nodes clean without APE native dependencies.

## Table of Contents
<!-- MarkdownTOC autolink="true" autoanchor="true" -->

- [Install](#install)
	- [1. Build and install arrow](#1-build-and-install-arrow)
	- [2. Build and install APE-native](#2-build-and-install-ape-native)
	- [3. Build and install pmem-common](#3-build-and-install-pmem-common)
	- [4. Build and install Intel Codec Library](#4-build-and-install-intel-codec-library)
	- [5. Build APE-java and start APE servers](#5-build-ape-java-and-start-ape-servers)
	- [6. Configure APE servers](#6-configure-ape-servers)

<!-- /MarkdownTOC -->

<a id="install"></a>
## Install

<a id="1-build-and-install-arrow"></a>
### 1. Build and install arrow
***(on each node of APE servers)***

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

<a id="2-build-and-install-ape-native"></a>
### 2. Build and install APE-native
***(on each node of APE servers)***

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

<a id="3-build-and-install-pmem-common"></a>
### 3. Build and install pmem-common
***(on each node of APE servers)***

```
git clone https://github.com/oap-project/pmem-common.git
cd pmem-common
git checkout branch-1.1-spark-3.x


mvn clean install -DskipTests

```

<a id="4-build-and-install-intel-codec-library"></a>
### 4. Build and install Intel Codec Library
***(on each node of APE servers)***

```
git clone https://github.com/Intel-bigdata/IntelCodecLibrary.git
cd IntelCodecLibrary


mvn clean install -DskipTests

```



<a id="5-build-ape-java-and-start-ape-servers"></a>
### 5. Build APE-java and start APE servers
***(on each node of APE servers)***

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

<a id="6-configure-ape-servers"></a>
### 6. Configure APE servers

Logs in above step will show `port` that `ape-server` is listening on. 

All `ip:port` of `ape-server`s need to be set in job configurations on client node of Spark/Flink.

***(config in $HADOOP_HOME/etc/hadoop/hdfs-site.xml on `client-node`)***

```
    <property>
        <name>fs.ape.client.remote.servers</name>
        <value>host1:port1,host2:port2</value>
    </property>
```
