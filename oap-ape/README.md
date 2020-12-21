#Dependency
[comment]: <>(TODO: can use some easier way?)
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
mdkir build
cd build
cmake ..
make
```
## parquet-test demo
For parquet-test demo, make sure you have install libarrow, libparquet. Arrow build and install command:
```
git clone https://github.com/Intel-bigdata/arrow.git
cd arrow && git checkout branch-0.17.0-oap-1.0
mkdir -p cpp/release-build
cd cpp/release-build
ARROW_INSTALL_DIR=/usr # it's better to install to a user private dir and manually add path to LD_LIBRARY_PATH
, CPLUS_INCLUDE_PATH
cmake -DCMAKE_INSTALL_PREFIX=$ARROW_INSTALL_DIR -DARROW_PARQUET=ON -DARROW_HDFS=ON -DARROW_JNI=ON -DARROW_FILESYSTEM
=ON -DARROW_DEPENDENCY_SOURCE=BUNDLED ..
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
