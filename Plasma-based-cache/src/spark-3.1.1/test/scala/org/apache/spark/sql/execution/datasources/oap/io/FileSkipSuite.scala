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

package org.apache.spark.sql.execution.datasources.oap.io

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils

class FileSkipSuite extends QueryTest with SharedOapContext with BeforeAndAfterEach {
  import testImplicits._

  override def beforeEach(): Unit = {
    val path1 = Utils.createTempDir().getAbsolutePath

    sql(s"""CREATE TEMPORARY VIEW parquet_test (a INT, b STRING)
           | USING parquet
           | OPTIONS (path '$path1')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("parquet_test")
  }

  test("skip all file (is not null)") {
    val data: Seq[(Int, String)] =
      scala.util.Random.shuffle(1 to 300).map(i => (i, null)).toSeq
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    val result = sql("SELECT * FROM parquet_test WHERE b is not null")
    assert(result.count == 0)
  }

  test("skip all file (equal)") {
    val data: Seq[(Int, String)] =
      scala.util.Random.shuffle(1 to 300).map(i => (i, s"this is test $i")).toSeq
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    val result1 = sql("SELECT * FROM parquet_test WHERE a = 1")
    assert(result1.count == 1)
    val result2 = sql("SELECT * FROM parquet_test WHERE a = 500")
    assert(result2.count == 0)
  }

  test("skip all file (lt)") {
    val data: Seq[(Int, String)] =
      scala.util.Random.shuffle(1 to 300).map(i => (i, s"this is test $i")).toSeq
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    val result1 = sql("SELECT * FROM parquet_test WHERE a < 1")
    assert(result1.count == 0)
    val result2 = sql("SELECT * FROM parquet_test WHERE a < 2")
    assert(result2.count == 1)
  }

  test("skip all file (lteq)") {
    val data: Seq[(Int, String)] =
      scala.util.Random.shuffle(1 to 300).map(i => (i, s"this is test $i")).toSeq
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    val result1 = sql("SELECT * FROM parquet_test WHERE a <= 0")
    assert(result1.count == 0)
    val result2 = sql("SELECT * FROM parquet_test WHERE a <= 1")
    assert(result2.count == 1)
  }

  test("skip all file (gt)") {
    val data: Seq[(Int, String)] =
      scala.util.Random.shuffle(1 to 300).map(i => (i, s"this is test $i")).toSeq
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    val result1 = sql("SELECT * FROM parquet_test WHERE a > 300")
    assert(result1.count == 0)
    val result2 = sql("SELECT * FROM parquet_test WHERE a > 2")
    assert(result2.count == 298)
  }

  test("skip all file (gteq)") {
    val data: Seq[(Int, String)] =
      scala.util.Random.shuffle(1 to 300).map(i => (i, s"this is test $i")).toSeq
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    val result1 = sql("SELECT * FROM parquet_test WHERE a >= 300")
    assert(result1.count == 1)
    val result2 = sql("SELECT * FROM parquet_test WHERE a >= 500")
    assert(result2.count == 0)
  }
}
