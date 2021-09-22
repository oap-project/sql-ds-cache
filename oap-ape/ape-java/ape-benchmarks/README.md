# Build
```
mvn clean package
```

# Usages

## Flink TPC-H
0. Data generation with [spark-sql-perf](https://github.com/databricks/spark-sql-perf/blob/master/src/main/notebooks/TPC-multi_datagen.scala)

1. Create database in Hive

2. Copy the corresponding `ape-flink` library jar into $FLINK_DIR/lib

3. Start a Flink session on Hadoop Yarn

4. Run test script


**Example**

```shell script
export FLINK_DIR=~/flink-1.12.0
export TPCH_TEST_DIR=~/sql-ds-cache/oap-ape/ape-java/ape-benchmarks/ape-benchmark-flink-tpch
export HIVE_DATABASE_NAME="tpch_1t_snappy"
export RESULT_DIR="hdfs:///flink/tmp/tpch_result"

# run script with parameters: yarn_application_id, queries, parallelism, enable_APE_features
sh ~/sql-ds-cache/oap-ape/ape-java/ape-benchmarks/test-scripts/test_flink_tpch.sh application_1629912933141_0001 1,2,3 100 false
```


**Note**

For flink `1.13.2`, before tests, you need:
 
(1) change `flink.version` in [pom.xml](ape-benchmark-flink-tpch/pom.xml)

(2) and change `SqlDialect.HIVE` to `SqlDialect.DEFAULT` in [TpchTestProgram.java](ape-benchmark-flink-tpch/src/main/java/org/apache/flink/table/tpch/TpchTestProgram.java)

(3) rebuild `ape-benchmark-flink-tpch`


## Flink TPC-DS
0. Data generation with [spark-sql-perf](https://github.com/databricks/spark-sql-perf/blob/master/src/main/notebooks/TPC-multi_datagen.scala)

1. Create database in Hive

2. Copy the corresponding `ape-flink` library jar into $FLINK_DIR/lib

3. Start a Flink session on Hadoop Yarn

4. Run test script


**Example**

```shell script
export FLINK_DIR=~/flink-1.12.0
export TPCDS_TEST_DIR=~/sql-ds-cache/oap-ape/ape-java/ape-benchmarks/ape-benchmark-flink-tpcds
export HIVE_DATABASE_NAME="tpcds_1t_snappy"
export RESULT_DIR="hdfs:///flink/tmp/tpcds_result"

# run script with parameters: yarn_application_id, queries, parallelism, enable_APE_features
sh ~/sql-ds-cache/oap-ape/ape-java/ape-benchmarks/test-scripts/test_flink_tpcds.sh application_1629912933141_0001 1,2,3 100 false
```


**Note**

For flink `1.13.2`, before tests, you need:
 
(1) change `flink.version` in [pom.xml](ape-benchmark-flink-tpcds/pom.xml)

(2) and change `SqlDialect.HIVE` to `SqlDialect.DEFAULT` in [TpcdsTestProgram.java](ape-benchmark-flink-tpcds/src/main/java/org/apache/flink/table/tpcds/TpcdsTestProgram.java)

(3) rebuild `ape-benchmark-flink-tpcds`
