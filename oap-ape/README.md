#Dependency
[comment]: <>(TODO: can use some easier way?)

## [arrow](https://gitlab.devtools.intel.com/POAE/arrow/-/tree/arrow-3.0.0-internal)
We use arrow to load parquet file data natively from HDFS.

We also use arrow-plasma as a KV store for caching. arrow-parquet is modified for caching.

## json

APE is using [nlohmann/json](https://github.com/nlohmann/json) lib to parse json between java and native. This is a
 header-only json lib. You need to pre-install this library on your work nodes.
 
 build and install commands:
 ```
git clone https://github.com/nlohmann/json
cd json
mkdir build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/your/library/path/
make -j
make install
```

## libhdfs

**libhdfs version**

`libhdfs` in `$HADOOP_HOME/lib/native` should be upgraded if you don't choose `libhdfs3`.

`libhdfs3` may enlarge data sizes of reading in some cases.

If you decide to update `libhdfs`, you can use the one under `resources/libhdfs/hadoop-3.2.2/`.

You can also copy it from the official `hadoop-3.2.2` package.
Hadoop release site: [Apache Hadoop](https://hadoop.apache.org/releases.html).


**JDK version**

`libhdfs` must run in newer versions of JDK.

Otherwise, core dump may occur occasionally when thread are existing.

We tested the following JDK which works well with `libhdfs`:

```
openjdk version "1.8.0_282"
```
Download site: [AdoptOpenJDK](https://adoptopenjdk.net/releases.html?variant=openjdk8&jvmVariant=openj9)

## Other Dependencies
* [openssl](https://www.openssl.org/) (We use it to compute hash when creating object ids of plasma)
* [redis-plus-plus](https://github.com/sewenew/redis-plus-plus) (We use redis to save locations in cluster of cached file data)

# How to build

java build
```
cd $OAP_ROOT_DIR/oap-ape/ape-java
mvn clean package -am
# test jni: you may need to change params in test code.
java -cp ape-jni/target/ape-jni-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.intel.ape.ParquetReaderTest
```

cpp build
```
cd $OAP_ROOT_DIR/oap-ape/ape-native/
mkdir build
cd build
cmake ..
make
```

Before you commit cpp code, please run this command to keep code style clean(you need to install clang-format).
```
cd oap-ape/ape-native/ 
python3 ./build-support/run_clang_format.py --clang_format_binary clang-format --source_dir ./src/  --fix
```
## parquet-test demo
For parquet-test demo, make sure you have installed libarrow, libparquet, etc. Arrow build and install command:
```
git clone https://gitlab.devtools.intel.com/POAE/arrow.git
cd arrow && git checkout arrow-3.0.0-internal
mkdir -p cpp/release-build
cd cpp/release-build
ARROW_INSTALL_DIR=/usr # it's better to install to a user private dir and manually add path to LD_LIBRARY_PATH
, CPLUS_INCLUDE_PATH
cmake -DCMAKE_INSTALL_PREFIX=$ARROW_INSTALL_DIR -DARROW_PARQUET=ON -DARROW_HDFS=ON -DARROW_JNI=ON -DARROW_FILESYSTEM=ON \
-DARROW_ORC=ON -DARROW_PLASMA_JAVA_CLIENT=on -DARROW_PLASMA=on -DARROW_DEPENDENCY_SOURCE=BUNDLED ..
make -j 
make install
```

Make sure you have set up `$HADOOP_HOME` environment
 variables. make sure libjvm.so in your `$LD_LIBRARY_PATH`.  
 
 If you got errors like below
 ```
loadFileSystems error:
(unable to get stack trace for java.lang.NoClassDefFoundError exception: ExceptionUtils::getStackTrace error.)
hdfsBuilderConnect(forceNewInstance=0, nn=default, port=0, kerbTicketCachePath=(NULL), userName=(NULL)) error:
(unable to get stack trace for java.lang.NoClassDefFoundError exception: ExceptionUtils::getStackTrace error.)
hdfsOpenFile(/tmp/testfile.txt): constructNewObjectOfPath error:
(unable to get stack trace for java.lang.NoClassDefFoundError exception: ExceptionUtils::getStackTrace error.)
Failed to open /tmp/testfile.txt for writing!
```
please execute commands(see [link](https://stackoverflow.com/questions/21064140/hadoop-c-hdfs-test-running-exception)):
```
export CLASSPATH=${HADOOP_HOME}/etc/hadoop:`find ${HADOOP_HOME}/share/hadoop/ | awk '{path=path":"$0}END{print path}'`
export LD_LIBRARY_PATH="${HADOOP_HOME}/lib/native":$LD_LIBRARY_PATH
```
## use libhdfs3.so to replace libhdfs.so
We found that java side call ParquetReaderJNI will throw some error, haven't found root cause yet. But a guess is that libhdfs will call java code in the end, call stack is like 
 ``` 
 java(test code) -> jni(reader JNI) -> native(Reader ) -> jni (libhdfs ) -> java(hadoop) .
``` 

An option is to replace libhdfs.so with [libhdfs3.so](https://github.com/erikmuttersbach/libhdfs3) which is a pure native impl, and get rid of drawbacks of JNI.
 
you can refer this [link](https://github.com/Intel-bigdata/OAP/blob/master/oap-data-source/arrow/README.md#use-libhdfs3-library-for-better-performanceoptional) to install libhdfs3

when run test code, add below environment variable:
```
export LD_LIBRARY_PATH="${HADOOP_HOME}/lib/native":$LD_LIBRARY_PATH
export LIBHDFS3_CONF=/path/to/your/hdfs-site.xml
```

### hdfs extra configuration for shortcircurt read
add following configuration to `hdfs-site.xml` file, and make sure the access permission of `/var/lib/hadoop-hdfs` is 755.
```
    <property>
      <name>dfs.client.read.shortcircuit</name>
      <value>true</value>
    </property>
    <property>
      <name>dfs.domain.socket.path</name>
      <value>/var/lib/hadoop-hdfs/dn_socket</value>
    </property>
```

### Native Parquet Reader Configuration
##### columnarReaderBatchSize
Currently we reuse spark's original configuration ```spark.sql.parquet.columnarReaderBatchSize``` to avoid additional code dependency.
Default batchSize is 4096, but according to our test result, set batchSize to a larger num can improve performance.
Set batchSize to 20480 is recommended, can tune this num according to real work load.

##### Disable Parquet Filter in Spark
If you are testing Spark With TPC-H or TPC-DS, please set 'spark.sql.parquet.filterPushdown' to false in spark-defaults.conf since String type filter is not supported for now. 
