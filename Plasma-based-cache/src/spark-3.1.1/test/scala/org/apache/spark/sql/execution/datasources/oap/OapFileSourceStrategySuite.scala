/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.oap

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, OapFileSourceScanExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.test.oap.{SharedOapContext, TestIndex}
import org.apache.spark.util.Utils

abstract class OapFileSourceStrategySuite extends QueryTest with SharedOapContext with
  BeforeAndAfterEach {

  // TODO move Parquet TestSuite from FilterSuite
  import testImplicits._

  protected def testTableName: String

  protected def fileFormat: String

  override def beforeEach(): Unit = {
    val path = Utils.createTempDir().getAbsolutePath
    sql(s"""CREATE TEMPORARY VIEW $testTableName (a INT, b STRING)
           | USING $fileFormat
           | OPTIONS (path '$path')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable(s"$testTableName")
  }

  protected def verifyOapProjectFilterScan(
      indexColumn: String,
      verifyFileFormat: FileFormat => Boolean,
      verifySparkPlan: (SparkPlan, SparkPlan) => Boolean): Unit = {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql(s"insert overwrite table $testTableName select * from t")

    withIndex(TestIndex(s"$testTableName", "index1")) {
      sql(s"create oindex index1 on $testTableName ($indexColumn)")
      val plan =
        sql(s"SELECT b FROM $testTableName WHERE b = 'this is test 1'")
          .queryExecution.optimizedPlan
      val optimizedSparkPlans = OapFileSourceStrategy(plan)
      assert(optimizedSparkPlans.size == 1)

      val optimizedSparkPlan = optimizedSparkPlans.head
      assert(optimizedSparkPlan.isInstanceOf[ProjectExec])
      assert(optimizedSparkPlan.children.nonEmpty)
      assert(optimizedSparkPlan.children.length == 1)

      val filter = optimizedSparkPlan.children.head
      assert(filter.isInstanceOf[FilterExec])
      assert(filter.children.nonEmpty)
      assert(filter.children.length == 1)

      val scan = filter.children.head
      assert(scan.isInstanceOf[OapFileSourceScanExec])
      val relation = scan.asInstanceOf[OapFileSourceScanExec].relation
      assert(relation.isInstanceOf[HadoopFsRelation])
      assert(verifyFileFormat(relation.fileFormat))

      val sparkPlans = FileSourceStrategy(plan)
      assert(sparkPlans.size == 1)
      val sparkPlan = sparkPlans.head
      assert(verifySparkPlan(sparkPlan, optimizedSparkPlan))
    }
  }

  protected def verifyProjectFilterScan(
      indexColumn: String,
      verifyFileFormat: FileFormat => Boolean,
      verifySparkPlan: (SparkPlan, SparkPlan) => Boolean): Unit = {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql(s"insert overwrite table $testTableName select * from t")

    withIndex(TestIndex(s"$testTableName", "index1")) {
      sql(s"create oindex index1 on $testTableName ($indexColumn)")
      val plan =
        sql(s"SELECT b FROM $testTableName WHERE b = 'this is test 1'")
          .queryExecution.optimizedPlan
      val optimizedSparkPlans = OapFileSourceStrategy(plan)
      assert(optimizedSparkPlans.size == 1)

      val optimizedSparkPlan = optimizedSparkPlans.head
      assert(optimizedSparkPlan.isInstanceOf[ProjectExec])
      assert(optimizedSparkPlan.children.nonEmpty)
      assert(optimizedSparkPlan.children.length == 1)

      val filter = optimizedSparkPlan.children.head
      assert(filter.isInstanceOf[FilterExec])
      assert(filter.children.nonEmpty)
      assert(filter.children.length == 1)

      val scan = filter.children.head
      assert(scan.isInstanceOf[FileSourceScanExec])
      val relation = scan.asInstanceOf[FileSourceScanExec].relation
      assert(relation.isInstanceOf[HadoopFsRelation])
      assert(verifyFileFormat(relation.fileFormat))

      val sparkPlans = FileSourceStrategy(plan)
      assert(sparkPlans.size == 1)
      val sparkPlan = sparkPlans.head
      assert(verifySparkPlan(sparkPlan, optimizedSparkPlan))
    }
  }

  protected def verifyOapProjectScan(
      verifyFileFormat: FileFormat => Boolean,
      verifySparkPlan: (SparkPlan, SparkPlan) => Boolean): Unit = {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql(s"insert overwrite table $testTableName select * from t")

    val plan = sql(s"SELECT a FROM $testTableName").queryExecution.optimizedPlan
    val optimizedSparkPlans = OapFileSourceStrategy(plan)
    assert(optimizedSparkPlans.size == 1)

    val optimizedSparkPlan = optimizedSparkPlans.head
    assert(optimizedSparkPlan.isInstanceOf[ProjectExec])
    assert(optimizedSparkPlan.children.nonEmpty)
    assert(optimizedSparkPlan.children.length == 1)

    val scan = optimizedSparkPlan.children.head
    assert(scan.isInstanceOf[OapFileSourceScanExec])
    val relation = scan.asInstanceOf[OapFileSourceScanExec].relation
    assert(relation.isInstanceOf[HadoopFsRelation])
    assert(verifyFileFormat(relation.fileFormat))

    val sparkPlans = FileSourceStrategy(plan)
    assert(sparkPlans.size == 1)
    val sparkPlan = sparkPlans.head
    assert(verifySparkPlan(sparkPlan, optimizedSparkPlan))
  }

  protected def verifyProjectScan(
      verifyFileFormat: FileFormat => Boolean,
      verifySparkPlan: (SparkPlan, SparkPlan) => Boolean): Unit = {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql(s"insert overwrite table $testTableName select * from t")

    val plan = sql(s"SELECT a FROM $testTableName").queryExecution.optimizedPlan
    val optimizedSparkPlans = OapFileSourceStrategy(plan)
    assert(optimizedSparkPlans.size == 1)

    val optimizedSparkPlan = optimizedSparkPlans.head
    assert(optimizedSparkPlan.isInstanceOf[ProjectExec])
    assert(optimizedSparkPlan.children.nonEmpty)
    assert(optimizedSparkPlan.children.length == 1)

    val scan = optimizedSparkPlan.children.head
    assert(scan.isInstanceOf[FileSourceScanExec])
    val relation = scan.asInstanceOf[FileSourceScanExec].relation
    assert(relation.isInstanceOf[HadoopFsRelation])
    assert(verifyFileFormat(relation.fileFormat))

    val sparkPlans = FileSourceStrategy(plan)
    assert(sparkPlans.size == 1)
    val sparkPlan = sparkPlans.head
    assert(verifySparkPlan(sparkPlan, optimizedSparkPlan))
  }

  protected def verifyScan(
      verifyFileFormat: FileFormat => Boolean,
      verifySparkPlan: (SparkPlan, SparkPlan) => Boolean): Unit = {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql(s"insert overwrite table $testTableName select * from t")

    val plan = sql(s"FROM $testTableName").queryExecution.optimizedPlan
    val optimizedSparkPlans = OapFileSourceStrategy(plan)
    assert(optimizedSparkPlans.size == 1)
    val optimizedSparkPlan = optimizedSparkPlans.head
    assert(optimizedSparkPlan.isInstanceOf[OapFileSourceScanExec])
    val relation = optimizedSparkPlan.asInstanceOf[OapFileSourceScanExec].relation
    assert(verifyFileFormat(relation.fileFormat))

    val sparkPlans = FileSourceStrategy(plan)
    assert(sparkPlans.size == 1)
    val sparkPlan = sparkPlans.head

    assert(verifySparkPlan(sparkPlan, optimizedSparkPlan))
  }
}

class OapFileSourceStrategyForParquetSuite extends OapFileSourceStrategySuite {
  protected def testTableName: String = "parquet_test"

  protected def fileFormat: String = "parquet"

  test("Project-> Filter -> Scan : Optimized") {
    verifyOapProjectFilterScan(
      indexColumn = "b",
      format => format.isInstanceOf[OptimizedParquetFileFormat],
      (plan1, plan2) => !plan1.sameResult(plan2)
    )
  }

  test("Project-> Filter -> Scan : Not Optimized") {
    verifyProjectFilterScan(
      indexColumn = "a",
      format => format.isInstanceOf[ParquetFileFormat],
      (plan1, plan2) => plan1.sameResult(plan2)
    )
  }

  test("Project -> Scan : Optimized") {
    withSQLConf(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key -> "true") {
      verifyOapProjectScan(
        format => format.isInstanceOf[OptimizedParquetFileFormat],
        (plan1, plan2) => !plan1.sameResult(plan2)
      )
    }
  }

  test("Project -> Scan : Not Optimized") {
    verifyProjectScan(
      format => format.isInstanceOf[ParquetFileFormat],
      (plan1, plan2) => plan1.sameResult(plan2)
    )
  }

  test("Disable Optimized") {
    withSQLConf(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED.key -> "true",
      OapConf.OAP_CACHE_RUNTIME_ENABLED.key -> "false") {
      verifyProjectScan(
        format => format.isInstanceOf[ParquetFileFormat],
        (plan1, plan2) => plan1.sameResult(plan2)
      )
      verifyProjectFilterScan(
        indexColumn = "b",
        format => format.isInstanceOf[ParquetFileFormat],
        (plan1, plan2) => plan1.sameResult(plan2)
      )
    }
  }

  test("simple inner join triggers DPP with mock-up tables") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true") {
      withTable("df1", "df2") {
        spark.range(1000)
          .select(col("id"), col("id").as("k"))
          .write
          .partitionBy("k")
          .format(fileFormat)
          .mode("overwrite")
          .saveAsTable("df1")

        spark.range(100)
          .select(col("id"), col("id").as("k"))
          .write
          .partitionBy("k")
          .format(fileFormat)
          .mode("overwrite")
          .saveAsTable("df2")

        val df = sql("SELECT df1.id, df2.k FROM df1 JOIN df2 ON df1.k = df2.k AND df2.id < 2")
        checkAnswer(df, Row(0, 0) :: Row(1, 1) :: Nil)
      }
    }
  }
}

class OapFileSourceStrategyForOrcSuite extends OapFileSourceStrategySuite {
  protected def testTableName: String = "orc_test"

  protected def fileFormat: String = "orc"

  test("Project-> Filter -> Scan : Optimized") {
    verifyOapProjectFilterScan(
      indexColumn = "b",
      format => format.isInstanceOf[OptimizedOrcFileFormat],
      (plan1, plan2) => !plan1.sameResult(plan2)
    )
  }

  test("Project-> Filter -> Scan : Not Optimized") {
    verifyProjectFilterScan(
      indexColumn = "a",
      format => format.isInstanceOf[OrcFileFormat],
      (plan1, plan2) => plan1.sameResult(plan2)
    )
  }

  test("Project -> Scan : Not Optimized") {
    verifyProjectScan(
      format => format.isInstanceOf[OrcFileFormat],
      (plan1, plan2) => plan1.sameResult(plan2)
    )
  }

  test("simple inner join triggers DPP with mock-up tables") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true") {
      withTable("df1", "df2") {
        spark.range(1000)
          .select(col("id"), col("id").as("k"))
          .write
          .partitionBy("k")
          .format(fileFormat)
          .mode("overwrite")
          .saveAsTable("df1")

        spark.range(100)
          .select(col("id"), col("id").as("k"))
          .write
          .partitionBy("k")
          .format(fileFormat)
          .mode("overwrite")
          .saveAsTable("df2")

        val df = sql("SELECT df1.id, df2.k FROM df1 JOIN df2 ON df1.k = df2.k AND df2.id < 2")
        checkAnswer(df, Row(0, 0) :: Row(1, 1) :: Nil)
      }
    }
  }
}
