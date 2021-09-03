# Developer Guide

This document is a supplement to the whole [OAP Developer Guide](OAP-Developer-Guide.md) for SQL Index and Data Source Cache.
After following that document, you can continue more details for SQL Index and Data Source Cache.

## Building

### Prerequisites for building 

Building with [Apache Maven\*](http://maven.apache.org/).

Before building, install PMem-Common locally:

```
git clone -b <tag-version> https://github.com/oap-project/pmem-common.git
cd pmem-common
mvn clean install -DskipTests
```

Install the required packages on the build system:

- [cmake](https://cmake.org/install/)
- [Plasma](http://arrow.apache.org/blog/2017/08/08/plasma-in-memory-object-store/)

#### Plasma installation

To use optimized Plasma cache with OAP, you need following components:  

   (1) `libarrow.so`, `libplasma.so`, `libplasma_java.so`: dynamic libraries, will be used in Plasma client.   
   (2) `plasma-store-server`: executable file, Plasma cache service.  
   (3) `arrow-plasma-4.0.0.jar`: will be used when compile oap and spark runtime also need it. 

- `.so` file and binary file  
  Clone code from Arrow repo and run following commands, this will install `libplasma.so`, `libarrow.so`, `libplasma_java.so` and `plasma-store-server` to your system path(`/usr/lib64` by default). And if you are using Spark in a cluster environment, you can copy these files to all nodes in your cluster if the OS or distribution are same, otherwise, you need compile it on each node.
  
```
cd /tmp
git clone https://github.com/oap-project/arrow.git
cd arrow && git checkout v4.0.0-oap-1.2.0
cd cpp
mkdir release
cd release
#build libarrow, libplasma, libplasma_java
cmake -DCMAKE_INSTALL_PREFIX=/usr/ -DCMAKE_BUILD_TYPE=Release -DARROW_BUILD_TESTS=on -DARROW_PLASMA_JAVA_CLIENT=on -DARROW_PLASMA=on -DARROW_DEPENDENCY_SOURCE=BUNDLED  ..
make -j$(nproc)
sudo make install -j$(nproc)
```

- arrow-plasma-4.0.0.jar  
  Run following command, this will install arrow jars to your local maven repo. Besides, you need copy arrow-plasma-4.0.0.jar to `$SPARK_HOME/jars/` dir, cause this jar is needed when using external cache.
   
```
cd /tmp/arrow/java
mvn clean -q -pl plasma -am -DskipTests install
```


Build the SQL DS Cache package:

```
git clone -b <tag-version> https://github.com/oap-project/sql-ds-cache.git
cd sql-ds-cache
mvn clean -DskipTests package
```

### Running Tests

Run all the tests:
```
mvn clean test
```
Run a specific test suite, for example `OapDDLSuite`:
```
mvn -DwildcardSuites=org.apache.spark.sql.execution.datasources.oap.OapDDLSuite test
```
**NOTE**: Log level of unit tests currently default to ERROR, please override `sql-ds-cache/Plasma-based-cache/src/test/resources/log4j.properties` if needed.



###### \*Other names and brands may be claimed as the property of others.
