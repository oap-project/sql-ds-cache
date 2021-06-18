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
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils

class CombiningIndexSuite extends QueryTest with SharedOapContext with BeforeAndAfterEach{
  import testImplicits._
  private var currentPath: String = _

  override def beforeEach(): Unit = {
    spark.conf.set(OapConf.OAP_INDEXER_CHOICE_MAX_SIZE.key, "2")
    spark.conf.set(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.key, "true")
    spark.conf.set(OapConf.OAP_INDEX_FILE_SIZE_MAX_RATIO.key, "1000")
    val path = Utils.createTempDir().getAbsolutePath
    currentPath = path

    sql(s"""CREATE TEMPORARY VIEW parquet_test (a INT, b INT, c INT)
           | USING parquet
           | OPTIONS (path '$path')""".stripMargin)

    sql(s"""CREATE TEMPORARY VIEW orc_test (a INT, b INT, c INT)
           | USING orc
           | OPTIONS (path '$path')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("parquet_test")
    sqlContext.dropTempTable("orc_test")
    spark.conf.unset(OapConf.OAP_INDEXER_CHOICE_MAX_SIZE.key)
    spark.conf.unset(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.key)
    spark.conf.unset(OapConf.OAP_INDEX_FILE_SIZE_MAX_RATIO.key)
  }

  test("filtering parquet") {
    val data: Seq[(Int, Int, Int)] = (1 to 200).map { i => (i % 13, (300 - i) % 17, i) }
    data.toDF("a", "b", "c").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    sql("create oindex index1 on parquet_test (a)")
    sql("create oindex index2 on parquet_test (b) USING BITMAP")

    checkAnswer(sql("SELECT c FROM parquet_test WHERE a = 1 and b = 10"),
      Row(1) :: Nil)

    checkAnswer(sql("SELECT c FROM parquet_test WHERE a > 3 and a < 5 and b = 14"),
      Row(82) :: Nil)

    checkAnswer(sql("SELECT c FROM parquet_test WHERE a > 3 and a < 5 and b = 14 and c > 30"),
      Row(82) :: Nil)

    sql("drop oindex index1 on parquet_test")
    sql("drop oindex index2 on parquet_test")

    sql("create oindex index1 on parquet_test (a,b)")
    sql("create oindex index2 on parquet_test (c)")

    checkAnswer(sql("SELECT c FROM parquet_test WHERE a = 3 and b > 14 and c > 30"),
      Row(81) :: Nil)

    checkAnswer(sql("SELECT c FROM parquet_test WHERE a = 3 and b > 14 and c > 300"),
      Nil)

    sql("drop oindex index1 on parquet_test")
    sql("drop oindex index2 on parquet_test")
  }

  test("filtering orc") {
    val data: Seq[(Int, Int, Int)] = (1 to 200).map { i => (i % 13, (300 - i) % 17, i) }
    data.toDF("a", "b", "c").createOrReplaceTempView("t")
    sql("insert overwrite table orc_test select * from t")
    sql("create oindex index1 on orc_test (a)")
    sql("create oindex index2 on orc_test (b)  USING BITMAP")

    checkAnswer(sql("SELECT c FROM orc_test WHERE a = 1 and b = 10"),
      Row(1) :: Nil)

    checkAnswer(sql("SELECT c FROM orc_test WHERE a > 3 and a < 5 and b = 14"),
      Row(82) :: Nil)

    checkAnswer(sql("SELECT c FROM orc_test WHERE a > 3 and a < 5 and b = 14 and c > 30"),
      Row(82) :: Nil)

    sql("drop oindex index1 on orc_test")
    sql("drop oindex index2 on orc_test ")


    sql("create oindex index1 on orc_test (a,b)")
    sql("create oindex index2 on orc_test (c)")

    checkAnswer(sql("SELECT c FROM orc_test WHERE a = 3 and b > 14 and c > 30"),
      Row(81) :: Nil)

    checkAnswer(sql("SELECT c FROM orc_test WHERE a = 3 and b > 14 and c > 300"), Nil)

    sql("drop oindex index1 on orc_test")
    sql("drop oindex index2 on orc_test")
  }
}
