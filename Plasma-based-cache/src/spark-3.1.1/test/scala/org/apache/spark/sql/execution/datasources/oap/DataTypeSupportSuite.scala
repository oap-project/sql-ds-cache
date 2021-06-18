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

import java.sql.Date

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.test.oap.{SharedOapContext, TestIndex, TestPartition}

class DataTypeSupportSuite extends QueryTest with SharedOapContext with BeforeAndAfterEach {

  import testImplicits._

  override def beforeEach(): Unit = {
    // OrcFileFormat
    sql(s"""CREATE TABLE orc_partitioned_by_string (a int, b string)
           | USING orc
           | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE orc_partitioned_by_int (a int, b int)
           | USING orc
           | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE orc_partitioned_by_long (a int, b long)
           | USING orc
           | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE orc_partitioned_by_boolean (a int, b boolean)
           | USING orc
           | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE orc_partitioned_by_date (a int, b date)
           | USING orc
           | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE orc_partitioned_by_double (a int, b double)
           | USING orc
           | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE orc_partitioned_by_float (a int, b float)
           | USING orc
           | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE orc_partitioned_by_byte (a int, b byte)
           | USING orc
           | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE orc_partitioned_by_short (a int, b short)
           | USING orc
           | PARTITIONED by (b)""".stripMargin)
    // ParquetFileFormat
    sql(s"""CREATE TABLE parquet_partitioned_by_string (a int, b string)
           | USING parquet
           | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE parquet_partitioned_by_int (a int, b int)
           | USING parquet
           | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE parquet_partitioned_by_long (a int, b long)
           | USING parquet
           | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE parquet_partitioned_by_boolean (a int, b boolean)
           | USING parquet
           | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE parquet_partitioned_by_date (a int, b date)
           | USING parquet
           | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE parquet_partitioned_by_double (a int, b double)
           | USING parquet
           | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE parquet_partitioned_by_float (a int, b float)
           | USING parquet
           | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE parquet_partitioned_by_byte (a int, b byte)
           | USING parquet
           | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE parquet_partitioned_by_short (a int, b short)
           | USING parquet
           | PARTITIONED by (b)""".stripMargin)
  }

  override def afterEach(): Unit = {
    sql("DROP TABLE IF EXISTS orc_partitioned_by_string")
    sql("DROP TABLE IF EXISTS orc_partitioned_by_int")
    sql("DROP TABLE IF EXISTS orc_partitioned_by_long")
    sql("DROP TABLE IF EXISTS orc_partitioned_by_boolean")
    sql("DROP TABLE IF EXISTS orc_partitioned_by_date")
    sql("DROP TABLE IF EXISTS orc_partitioned_by_double")
    sql("DROP TABLE IF EXISTS orc_partitioned_by_float")
    sql("DROP TABLE IF EXISTS orc_partitioned_by_byte")
    sql("DROP TABLE IF EXISTS orc_partitioned_by_short")
    sql("DROP TABLE IF EXISTS parquet_partitioned_by_string")
    sql("DROP TABLE IF EXISTS parquet_partitioned_by_int")
    sql("DROP TABLE IF EXISTS parquet_partitioned_by_long")
    sql("DROP TABLE IF EXISTS parquet_partitioned_by_boolean")
    sql("DROP TABLE IF EXISTS parquet_partitioned_by_date")
    sql("DROP TABLE IF EXISTS parquet_partitioned_by_double")
    sql("DROP TABLE IF EXISTS parquet_partitioned_by_float")
    sql("DROP TABLE IF EXISTS parquet_partitioned_by_byte")
    sql("DROP TABLE IF EXISTS parquet_partitioned_by_short")
  }

  test("create index on table partitioned by string type") {
    withIndex(TestIndex("parquet_partitioned_by_string", "idx1")) {
      val data: Seq[(Int, String)] = (1 to 10).map { i => (i, (i%2).toString)}
      data.toDF("key", "value").createOrReplaceTempView("t")
      sql("insert overwrite table orc_partitioned_by_string select * from t")
      sql("create oindex idx1 on orc_partitioned_by_string (a) partition(b='1')")
      checkAnswer(sql("select * from orc_partitioned_by_string where a = 1"),
        Row(1, "1"):: Nil)
      sql("drop oindex idx1 on orc_partitioned_by_string partition(b='1')")

      sql("insert overwrite table parquet_partitioned_by_string select * from t")
      sql("create oindex idx1 on parquet_partitioned_by_string (a) partition(b='1')")
      checkAnswer(sql("select * from parquet_partitioned_by_string where a = 1"),
        Row(1, "1"):: Nil)
    }
  }

  test("create index on table partitioned by int type") {
    val data: Seq[(Int, Int)] = (1 to 10).map { i => (i, i%2)}
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("orc_partitioned_by_int", "idx1", TestPartition("b", "1"))) {
      sql("insert overwrite table orc_partitioned_by_int select * from t")
      sql("create oindex idx1 on orc_partitioned_by_int (a) partition(b=1)")
      checkAnswer(sql("select * from orc_partitioned_by_int where a = 1"),
        Row(1, 1):: Nil)
    }
    withIndex(TestIndex("parquet_partitioned_by_int", "idx1", TestPartition("b", "1"))) {
      sql("insert overwrite table parquet_partitioned_by_int select * from t")
      sql("create oindex idx1 on parquet_partitioned_by_int (a) partition(b=1)")
      checkAnswer(sql("select * from parquet_partitioned_by_int where a = 1"),
        Row(1, 1):: Nil)
    }

  }

  test("create index on table partitioned by long type") {
    val data: Seq[(Int, Long)] = (1 to 10).map { i => (i, (i%2).toLong)}
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("orc_partitioned_by_long", "idx1", TestPartition("b", "1"))) {
      sql("insert overwrite table orc_partitioned_by_long select * from t")
      sql("create oindex idx1 on orc_partitioned_by_long (a) partition(b=1)")
      checkAnswer(sql("select * from orc_partitioned_by_long where a = 1"),
        Row(1, 1L):: Nil)
    }
    withIndex(TestIndex("parquet_partitioned_by_long", "idx1", TestPartition("b", "1"))) {
      sql("insert overwrite table parquet_partitioned_by_long select * from t")
      sql("create oindex idx1 on parquet_partitioned_by_long (a) partition(b=1)")
      checkAnswer(sql("select * from parquet_partitioned_by_long where a = 1"),
        Row(1, 1L):: Nil)
    }
  }

  test("create index on table partitioned by boolean type") {
    val data: Seq[(Int, Boolean)] = (1 to 10).map { i => (i, i%2==0)}
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("orc_partitioned_by_boolean", "idx1", TestPartition("b", "false"))) {
      sql("insert overwrite table orc_partitioned_by_boolean select * from t")
      sql("create oindex idx1 on orc_partitioned_by_boolean (a) partition(b=false)")
      checkAnswer(sql("select * from orc_partitioned_by_boolean where a = 1"),
        Row(1, false):: Nil)
    }
    withIndex(TestIndex("parquet_partitioned_by_boolean", "idx1", TestPartition("b", "false"))) {
      sql("insert overwrite table parquet_partitioned_by_boolean select * from t")
      sql("create oindex idx1 on parquet_partitioned_by_boolean (a) partition(b=false)")
      checkAnswer(sql("select * from parquet_partitioned_by_boolean where a = 1"),
        Row(1, false):: Nil)
    }
  }

  test("create index on table partitioned by date type") {
    val data: Seq[(Int, Date)] = (1 to 10).map { i => (i, DateTimeUtils.toJavaDate(i%2))}
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("orc_partitioned_by_date", "idx1", TestPartition("b", "1970-01-01"))) {
      sql("insert overwrite table orc_partitioned_by_date select * from t")
      sql("create oindex idx1 on orc_partitioned_by_date (a) partition(b='1970-01-01')")
      checkAnswer(sql("select * from orc_partitioned_by_date where a = 1"),
        Row(1, DateTimeUtils.toJavaDate(1)):: Nil)
    }
    withIndex(TestIndex("parquet_partitioned_by_date", "idx1", TestPartition("b", "1970-01-01"))) {
      sql("insert overwrite table parquet_partitioned_by_date select * from t")
      sql("create oindex idx1 on parquet_partitioned_by_date (a) partition(b='1970-01-01')")
      checkAnswer(sql("select * from parquet_partitioned_by_date where a = 1"),
        Row(1, DateTimeUtils.toJavaDate(1)):: Nil)
    }
  }

  test("create index on table partitioned by double type") {
    val data: Seq[(Int, Double)] = (1 to 10).map { i => (i, (i%2).toDouble)}
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("orc_partitioned_by_double", "idx1", TestPartition("b", "1.0"))) {
      sql("insert overwrite table orc_partitioned_by_double select * from t")
      sql("create oindex idx1 on orc_partitioned_by_double (a) partition(b=1.0)")
      checkAnswer(sql("select * from orc_partitioned_by_double where a = 1"),
        Row(1, 1.0):: Nil)
    }
    withIndex(TestIndex("parquet_partitioned_by_double", "idx1", TestPartition("b", "1.0"))) {
      sql("insert overwrite table parquet_partitioned_by_double select * from t")
      sql("create oindex idx1 on parquet_partitioned_by_double (a) partition(b=1.0)")
      checkAnswer(sql("select * from parquet_partitioned_by_double where a = 1"),
        Row(1, 1.0):: Nil)
    }
  }

  test("create index on table partitioned by float type") {
    val data: Seq[(Int, Float)] = (1 to 10).map { i => (i, (i%2).toFloat)}
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("orc_partitioned_by_float", "idx1", TestPartition("b", "1.0"))) {
      sql("insert overwrite table orc_partitioned_by_float select * from t")
      sql("create oindex idx1 on orc_partitioned_by_float (a) partition(b=1.0)")
      checkAnswer(sql("select * from orc_partitioned_by_float where a = 1"),
        Row(1, 1.0f):: Nil)
    }
    withIndex(TestIndex("parquet_partitioned_by_float", "idx1", TestPartition("b", "1.0"))) {
      sql("insert overwrite table parquet_partitioned_by_float select * from t")
      sql("create oindex idx1 on parquet_partitioned_by_float (a) partition(b=1.0)")
      checkAnswer(sql("select * from parquet_partitioned_by_float where a = 1"),
        Row(1, 1.0f):: Nil)
    }
  }

  test("create index on table partitioned by byte type") {
    val data: Seq[(Int, Byte)] = (1 to 10).map { i => (i, (i%2).toByte)}
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("orc_partitioned_by_byte", "idx1", TestPartition("b", "1"))) {
      sql("insert overwrite table orc_partitioned_by_byte select * from t")
      sql("create oindex idx1 on orc_partitioned_by_byte (a) partition(b=1)")
      checkAnswer(sql("select * from orc_partitioned_by_byte where a = 1"),
        Row(1, 1.toByte):: Nil)
    }
    withIndex(TestIndex("parquet_partitioned_by_byte", "idx1", TestPartition("b", "1"))) {
      sql("insert overwrite table parquet_partitioned_by_byte select * from t")
      sql("create oindex idx1 on parquet_partitioned_by_byte (a) partition(b=1)")
      checkAnswer(sql("select * from parquet_partitioned_by_byte where a = 1"),
        Row(1, 1.toByte):: Nil)
    }
  }

  test("create index on table partitioned by short type") {
    val data: Seq[(Int, Short)] = (1 to 10).map { i => (i, (i%2).toShort)}
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("orc_partitioned_by_short", "idx1", TestPartition("b", "1"))) {
      sql("insert overwrite table orc_partitioned_by_short select * from t")
      sql("create oindex idx1 on orc_partitioned_by_short (a) partition(b=1)")
      checkAnswer(sql("select * from orc_partitioned_by_short where a = 1"),
        Row(1, 1.toShort):: Nil)
    }
    withIndex(TestIndex("parquet_partitioned_by_short", "idx1", TestPartition("b", "1"))) {
      sql("insert overwrite table parquet_partitioned_by_short select * from t")
      sql("create oindex idx1 on parquet_partitioned_by_short (a) partition(b=1)")
      checkAnswer(sql("select * from parquet_partitioned_by_short where a = 1"),
        Row(1, 1.toShort):: Nil)
    }
  }
}

