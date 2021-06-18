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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.test.oap.{SharedOapContext, TestIndex, TestPartition}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.Utils

class FilterSuite extends QueryTest with SharedOapContext with BeforeAndAfterEach {

  import testImplicits._

  private var currentPath: String = _
  private var defaultEis: Boolean = true

  override def beforeAll(): Unit = {
    super.beforeAll()
    // In this suite we don't want to skip index even if the cost is higher.
    defaultEis = sqlContext.conf.getConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION)
    sqlContext.conf.setConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION, false)
  }

  override def afterAll(): Unit = {
    sqlContext.conf.setConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION, defaultEis)
    super.afterAll()
  }

  override def beforeEach(): Unit = {

    val path = Utils.createTempDir().getAbsolutePath
    currentPath = path
    sql(s"""CREATE TEMPORARY VIEW parquet_test (a INT, b STRING)
           | USING parquet
           | OPTIONS (path '$path')""".stripMargin)
    sql(s"""CREATE TEMPORARY VIEW parquet_test_date (a INT, b DATE)
           | USING parquet
           | OPTIONS (path '$path')""".stripMargin)
    sql(s"""CREATE TEMPORARY VIEW orc_test (a INT, b STRING)
           | USING orc
           | OPTIONS (path '$path')""".stripMargin)
    sql(s"""CREATE TABLE t_refresh_parquet (a int, b int)
            | USING parquet
            | PARTITIONED by (b)""".stripMargin)
    sql(s"""CREATE TABLE t_refresh_orc (a int, b int)
            | USING orc
            | PARTITIONED by (b)""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("parquet_test")
    sqlContext.dropTempTable("parquet_test_date")
    sqlContext.dropTempTable("orc_test")
    sql("DROP TABLE IF EXISTS t_refresh")
    sql("DROP TABLE IF EXISTS t_refresh_parquet")
    sql("DROP TABLE IF EXISTS t_refresh_orc")
  }

  test("empty table") {
    checkAnswer(sql("SELECT * FROM parquet_test"), Nil)
  }

  test("insert into table") {
    val data: Seq[(Int, String)] = (1 to 3000).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    checkAnswer(sql("SELECT * FROM parquet_test"), Seq.empty[Row])
    sql("insert overwrite table parquet_test select * from t")
    checkAnswer(sql("SELECT * FROM parquet_test"), data.map { row => Row(row._1, row._2) })
  }

  test("test date type") {
    val data: Seq[(Int, Date)] = (1 to 3000).map { i => (i, DateTimeUtils.toJavaDate(i)) }
    data.toDF("key", "value").createOrReplaceTempView("d")
    sql("insert overwrite table parquet_test_date select * from d")
    checkAnswer(sql("SELECT * FROM parquet_test_date"), data.map {row => Row(row._1, row._2)})
  }

  test("filtering") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("create oindex index1 on parquet_test (a)")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 1"),
        Row(1, "this is test 1") :: Nil)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a > 1 AND a <= 3"),
        Row(2, "this is test 2") :: Row(3, "this is test 3") :: Nil)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a <= 2"),
        Row(1, "this is test 1") :: Row(2, "this is test 2") :: Nil)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a >= 300"),
        Row(300, "this is test 300") :: Nil)
    }
  }

  test("filtering2") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t where key = 1")
    sql("insert into table parquet_test select * from t where key = 2")
    sql("insert into table parquet_test select * from t where key = 3")
    sql("insert into table parquet_test select * from t where key = 4")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("create oindex index1 on parquet_test (a)")

      val checkPath = new Path(currentPath)
      val fs = checkPath.getFileSystem(new Configuration())
      val indexFiles = fs.globStatus(new Path(checkPath, "*.index"))
      assert(indexFiles.length == 4)

      checkAnswer(sql("SELECT * FROM `parquet_test` WHERE a = 1"),
        Row(1, "this is test 1") :: Nil)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a > 1 AND a <= 3"),
        Row(2, "this is test 2") :: Row(3, "this is test 3") :: Nil)
    }
  }

  test("filtering multi index") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("create oindex index1 on parquet_test (a, b)")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 150 and b < 'this is test 3'"),
        Row(150, "this is test 150") :: Nil)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 150 and b >= 'this is test 1'"),
        Row(150, "this is test 150") :: Nil)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 150 and b = 'this is test 150'"),
        Row(150, "this is test 150") :: Nil)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a > 299 and b < 'this is test 9'"),
        Row(300, "this is test 300") :: Nil)
    }
  }

  test("filtering parquet") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("create oindex index1 on parquet_test (a)")

      sql("SELECT * FROM parquet_test WHERE b = '1'")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 1"),
        Row(1, "this is test 1") :: Nil)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a > 1 AND a <= 3"),
        Row(2, "this is test 2") :: Row(3, "this is test 3") :: Nil)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a > 1 AND b = 'this is test 2'"),
        Row(2, "this is test 2") :: Nil)
    }
  }

  test("filtering parquet2") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t where key = 1")
    sql("insert into table parquet_test select * from t where key = 2")
    sql("insert into table parquet_test select * from t where key = 3")
    sql("insert into table parquet_test select * from t where key = 4")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("create oindex index1 on parquet_test (a)")

      val checkPath = new Path(currentPath)
      val fs = checkPath.getFileSystem(new Configuration())
      val indexFiles = fs.globStatus(new Path(checkPath, "*.index"))
      assert(indexFiles.length == 4)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 1"),
        Row(1, "this is test 1") :: Nil)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a > 1 AND a <= 3"),
        Row(2, "this is test 2") :: Row(3, "this is test 3") :: Nil)
    }
  }

  test("filtering orc") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table orc_test select * from t")
    withIndex(TestIndex("orc_test", "index1")) {
      sql("create oindex index1 on orc_test (a)")

      sql("SELECT * FROM orc_test WHERE b = '1'")

      checkAnswer(sql("SELECT * FROM orc_test WHERE a = 1"),
        Row(1, "this is test 1") :: Nil)

      checkAnswer(sql("SELECT * FROM orc_test WHERE a > 1 AND a <= 3"),
        Row(2, "this is test 2") :: Row(3, "this is test 3") :: Nil)

      checkAnswer(sql("SELECT * FROM orc_test WHERE a > 1 AND b = 'this is test 2'"),
        Row(2, "this is test 2") :: Nil)
    }
  }

  test("filtering orc2") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table orc_test select * from t where key = 1")
    sql("insert into table orc_test select * from t where key = 2")
    sql("insert into table orc_test select * from t where key = 3")
    sql("insert into table orc_test select * from t where key = 4")
    withIndex(TestIndex("orc_test", "index1")) {
      sql("create oindex index1 on orc_test (a)")

      val checkPath = new Path(currentPath)
      val fs = checkPath.getFileSystem(new Configuration())
      val indexFiles = fs.globStatus(new Path(checkPath, "*.index"))
      assert(indexFiles.length == 4)

      checkAnswer(sql("SELECT * FROM orc_test WHERE a = 1"),
        Row(1, "this is test 1") :: Nil)

      checkAnswer(sql("SELECT * FROM orc_test WHERE a > 1 AND a <= 3"),
        Row(2, "this is test 2") :: Row(3, "this is test 3") :: Nil)
    }
  }

  test("filtering orc using binary cache") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table orc_test select * from t")
    withIndex(TestIndex("orc_test", "index1")) {
      sql("create oindex index1 on orc_test (a)")
      withSQLConf(OapConf.OAP_ORC_BINARY_DATA_CACHE_ENABLED.key -> "true") {
        val cacheManager = OapRuntime.getOrCreate.fiberCacheManager
        val beforeQuery = cacheManager.dataCacheCount
        checkAnswer(sql("SELECT * FROM orc_test WHERE  b = 'this is test 2'"),
          Row(2, "this is test 2") :: Nil)
        assert(cacheManager.dataCacheCount - beforeQuery == 4)
      }
    }
  }

  test("test refresh in parquet format on same partition") {
    val data: Seq[(Int, Int)] = (1 to 100).map { i => (i, i) }
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("t_refresh_parquet", "index1")) {
      sql(
        """
          |INSERT OVERWRITE TABLE t_refresh_parquet
          |partition (b=1)
          |SELECT key from t where value < 4
        """.stripMargin)

      sql("create oindex index1 on t_refresh_parquet (a)")

      checkAnswer(sql("select * from t_refresh_parquet"),
        Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Nil)

      sql(
        """
          |INSERT INTO TABLE t_refresh_parquet
          |partition (b=1)
          |SELECT key from t where value == 4
        """.stripMargin)

      sql("refresh oindex on t_refresh_parquet")

      checkAnswer(sql("select * from t_refresh_parquet"),
        Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Row(4, 1) :: Nil)
    }
  }

  test("test refresh in parquet format on different partition") {
    val data: Seq[(Int, Int)] = (1 to 100).map { i => (i, i) }
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("t_refresh_parquet", "index1")) {
      sql(
        """
          |INSERT OVERWRITE TABLE t_refresh_parquet
          |partition (b=1)
          |SELECT key from t where value < 4
        """.stripMargin)

      sql("create oindex index1 on t_refresh_parquet (a)")

      checkAnswer(sql("select * from t_refresh_parquet"),
        Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Nil)

      sql(
        """
          |INSERT INTO TABLE t_refresh_parquet
          |partition (b=2)
          |SELECT key from t where value == 4
        """.stripMargin)

      sql(
        """
          |INSERT INTO TABLE t_refresh_parquet
          |partition (b=1)
          |SELECT key from t where value == 5
        """.stripMargin)

      sql("refresh oindex on t_refresh_parquet")

      checkAnswer(sql("select * from t_refresh_parquet"),
        Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Row(5, 1) :: Row(4, 2) :: Nil)
    }
  }

  test("test refresh in parquet format on a partition") {
    val data: Seq[(Int, Int)] = (1 to 100).map { i => (i, i) }
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("t_refresh_parquet", "index1", TestPartition("b", "1")),
      TestIndex("t_refresh_parquet", "index1", TestPartition("b", "2"))) {
      sql(
        """
          |INSERT OVERWRITE TABLE t_refresh_parquet
          |partition (b=1)
          |SELECT key from t where value < 3
        """.stripMargin)

      sql(
        """
          |INSERT INTO TABLE t_refresh_parquet
          |partition (b=2)
          |SELECT key from t where value == 2
        """.stripMargin)


      sql("create oindex index1 on t_refresh_parquet (a)")

      checkAnswer(sql("select * from t_refresh_parquet"),
        Row(1, 1) :: Row(2, 1) :: Row(2, 2) :: Nil)

      sql(
        """
          |INSERT INTO TABLE t_refresh_parquet
          |partition (b=2)
          |SELECT key from t where value == 3
        """.stripMargin)

      sql(
        """
          |INSERT INTO TABLE t_refresh_parquet
          |partition (b=3)
          |SELECT key from t where value == 4
        """.stripMargin)

      sql("refresh oindex on t_refresh_parquet partition (b=2)")

      val fs = new Path(currentPath).getFileSystem(new Configuration())
      val tablePath = sqlContext.conf.warehousePath + "/t_refresh_parquet/"
      // here the parquet file changed from 1 to 2, so the index file changed to 2
      assert(fs.globStatus(new Path(tablePath + "b=1/*.index")).length == 2)
      assert(fs.globStatus(new Path(tablePath + "b=2/*.index")).length == 2)
      assert(fs.globStatus(new Path(tablePath + "b=3/*.index")).length == 0)

      checkAnswer(sql("select * from t_refresh_parquet"),
        Row(1, 1) :: Row(2, 1) :: Row(2, 2) :: Row(3, 2) :: Row(4, 3) :: Nil)
    }
  }

  test("test refresh in orc format on the same partition") {
    val data: Seq[(Int, Int)] = (1 to 100).map { i => (i, i) }
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("t_refresh_orc", "index1")) {
      sql(
        """
          |INSERT OVERWRITE TABLE t_refresh_orc
          |partition (b=1)
          |SELECT key from t where value < 4
        """.stripMargin)

      sql("create oindex index1 on t_refresh_orc (a)")

      checkAnswer(sql("select * from t_refresh_orc"),
        Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Nil)

      sql(
        """
          |INSERT INTO TABLE t_refresh_orc
          |partition (b=1)
          |SELECT key from t where value == 4
        """.stripMargin)

      sql("refresh oindex on t_refresh_orc")

      checkAnswer(sql("select * from t_refresh_orc"),
        Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Row(4, 1) :: Nil)
    }
  }

  test("test refresh in orc format on different partitions") {
    val data: Seq[(Int, Int)] = (1 to 100).map { i => (i, i) }
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("t_refresh_orc", "index1")) {
      sql(
        """
          |INSERT OVERWRITE TABLE t_refresh_orc
          |partition (b=1)
          |SELECT key from t where value < 4
        """.stripMargin)

      sql("create oindex index1 on t_refresh_orc (a)")

      checkAnswer(sql("select * from t_refresh_orc"),
        Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Nil)

      sql(
        """
          |INSERT INTO TABLE t_refresh_orc
          |partition (b=2)
          |SELECT key from t where value == 4
        """.stripMargin)

      sql(
        """
          |INSERT INTO TABLE t_refresh_orc
          |partition (b=1)
          |SELECT key from t where value == 5
        """.stripMargin)

      sql("refresh oindex on t_refresh_orc")

      checkAnswer(sql("select * from t_refresh_orc"),
        Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Row(5, 1) :: Row(4, 2) :: Nil)
    }
  }

  test("test refresh in orc format on a partition") {
    val data: Seq[(Int, Int)] = (1 to 100).map { i => (i, i) }
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("t_refresh_orc", "index1", TestPartition("b", "1")),
      TestIndex("t_refresh_orc", "index1", TestPartition("b", "2"))) {
      sql(
        """
          |INSERT OVERWRITE TABLE t_refresh_orc
          |partition (b=1)
          |SELECT key from t where value < 3
        """.stripMargin)

      sql(
        """
          |INSERT INTO TABLE t_refresh_orc
          |partition (b=2)
          |SELECT key from t where value == 2
        """.stripMargin)


      sql("create oindex index1 on t_refresh_orc (a)")

      checkAnswer(sql("select * from t_refresh_orc"),
        Row(1, 1) :: Row(2, 1) :: Row(2, 2) :: Nil)

      sql(
        """
          |INSERT INTO TABLE t_refresh_orc
          |partition (b=2)
          |SELECT key from t where value == 3
        """.stripMargin)

      sql(
        """
          |INSERT INTO TABLE t_refresh_orc
          |partition (b=3)
          |SELECT key from t where value == 4
        """.stripMargin)

      sql("refresh oindex on t_refresh_orc partition (b=2)")

      val fs = new Path(currentPath).getFileSystem(new Configuration())
      val tablePath = sqlContext.conf.warehousePath + "/t_refresh_orc/"
      // here the parquet file changed from 1 to 2, so the index file changed to 2
      assert(fs.globStatus(new Path(tablePath + "b=1/*.index")).length == 2)
      assert(fs.globStatus(new Path(tablePath + "b=2/*.index")).length == 2)
      assert(fs.globStatus(new Path(tablePath + "b=3/*.index")).length == 0)

      checkAnswer(sql("select * from t_refresh_orc"),
        Row(1, 1) :: Row(2, 1) :: Row(2, 2) :: Row(3, 2) :: Row(4, 3) :: Nil)
    }
  }

  test("test refresh in oap format on same partition") {
    val data: Seq[(Int, Int)] = (1 to 100).map { i => (i, i) }
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("t_refresh_parquet", "index1")) {
      sql(
        """
          |INSERT OVERWRITE TABLE t_refresh_parquet
          |partition (b=1)
          |SELECT key from t where value < 4
        """.stripMargin)

      sql("create oindex index1 on t_refresh_parquet (a)")

      checkAnswer(sql("select * from t_refresh_parquet"),
        Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Nil)

      sql(
        """
          |INSERT INTO TABLE t_refresh_parquet
          |partition (b=1)
          |SELECT key from t where value == 4
        """.stripMargin)

      sql("refresh oindex on t_refresh_parquet")

      checkAnswer(sql("select * from t_refresh_parquet"),
        Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Row(4, 1) :: Nil)
    }
  }

  test("test refresh in oap format on different partition") {
    val data: Seq[(Int, Int)] = (1 to 100).map { i => (i, i) }
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("t_refresh_parquet", "index1")) {
      sql(
        """
          |INSERT OVERWRITE TABLE t_refresh_parquet
          |partition (b=1)
          |SELECT key from t where value < 4
        """.stripMargin)

      sql("create oindex index1 on t_refresh_parquet (a)")

      checkAnswer(sql("select * from t_refresh_parquet"),
        Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Nil)

      sql(
        """
          |INSERT INTO TABLE t_refresh_parquet
          |partition (b=2)
          |SELECT key from t where value == 4
        """.stripMargin)

      sql(
        """
          |INSERT INTO TABLE t_refresh_parquet
          |partition (b=1)
          |SELECT key from t where value == 5
        """.stripMargin)

      sql("refresh oindex on t_refresh_parquet")

      checkAnswer(sql("select * from t_refresh_parquet"),
        Row(1, 1) :: Row(2, 1) :: Row(3, 1) :: Row(4, 2) :: Row(5, 1) :: Nil)
    }
  }

  test("test refresh in oap format on a partition") {
    val data: Seq[(Int, Int)] = (1 to 100).map { i => (i, i) }
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("t_refresh_parquet", "index1", TestPartition("b", "1")),
      TestIndex("t_refresh_parquet", "index1", TestPartition("b", "2"))) {
      sql(
        """
          |INSERT OVERWRITE TABLE t_refresh_parquet
          |partition (b=1)
          |SELECT key from t where value < 3
        """.stripMargin)

      sql(
        """
          |INSERT INTO TABLE t_refresh_parquet
          |partition (b=2)
          |SELECT key from t where value == 2
        """.stripMargin)


      sql("create oindex index1 on t_refresh_parquet (a)")

      checkAnswer(sql("select * from t_refresh_parquet"),
        Row(1, 1) :: Row(2, 1) :: Row(2, 2) :: Nil)

      sql(
        """
          |INSERT INTO TABLE t_refresh_parquet
          |partition (b=2)
          |SELECT key from t where value == 3
        """.stripMargin)

      sql(
        """
          |INSERT INTO TABLE t_refresh_parquet
          |partition (b=3)
          |SELECT key from t where value == 4
        """.stripMargin)

      sql("refresh oindex on t_refresh_parquet partition (b=2)")

      val fs = new Path(currentPath).getFileSystem(new Configuration())
      val tablePath = sqlContext.conf.warehousePath + "/t_refresh_parquet/"
      // here the parquet file changed from 1 to 2, so the index file changed to 2
      assert(fs.globStatus(new Path(tablePath + "b=1/*.index")).length == 2)
      assert(fs.globStatus(new Path(tablePath + "b=2/*.index")).length == 2)
      assert(fs.globStatus(new Path(tablePath + "b=3/*.index")).length == 0)

      checkAnswer(sql("select * from t_refresh_parquet"),
        Row(1, 1) :: Row(2, 1) :: Row(2, 2) :: Row(3, 2) :: Row(4, 3) :: Nil)
    }
  }

  test("refresh table of oap format without partition") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("insert overwrite table parquet_test select * from t")
      sql("create oindex index1 on parquet_test (a)")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 1"),
        Row(1, "this is test 1") :: Nil)

      sql("insert into table parquet_test select * from t")
      val checkPath = new Path(currentPath)
      val fs = checkPath.getFileSystem(new Configuration())
      assert(fs.globStatus(new Path(checkPath, "*.index")).length == 2)

      sql("refresh oindex on parquet_test")
      assert(fs.globStatus(new Path(checkPath, "*.index")).length == 4)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 1"),
        Row(1, "this is test 1") :: Row(1, "this is test 1") :: Nil)
    }
  }

  test("refresh table of parquet format without partition") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("insert overwrite table parquet_test select * from t")
      sql("create oindex index1 on parquet_test (a)")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 1"),
        Row(1, "this is test 1") :: Nil)

      sql("insert into table parquet_test select * from t")
      sql("refresh oindex on parquet_test")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 1"),
        Row(1, "this is test 1") :: Row(1, "this is test 1") :: Nil)
    }
  }

  test("refresh table of orc format without partition") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("orc_test", "index1")) {
      sql("insert overwrite table orc_test select * from t")
      sql("create oindex index1 on orc_test (a)")

      checkAnswer(sql("SELECT * FROM orc_test WHERE a = 1"),
        Row(1, "this is test 1") :: Nil)

      sql("insert into table orc_test select * from t")
      sql("refresh oindex on orc_test")

      checkAnswer(sql("SELECT * FROM orc_test WHERE a = 1"),
        Row(1, "this is test 1") :: Row(1, "this is test 1") :: Nil)
    }
  }

  test("filtering by string") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("insert overwrite table parquet_test select * from t")
      sql("create oindex index1 on parquet_test (a) using btree")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 1"),
        Row(1, "this is test 1") :: Nil)

      sql("insert into table parquet_test select * from t")
      sql("refresh oindex on parquet_test")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 1"),
        Row(1, "this is test 1") :: Row(1, "this is test 1") :: Nil)
    }
  }

  test("support data append without refresh") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("insert overwrite table parquet_test select * from t where key > 100")
      sql("create oindex index1 on parquet_test (a)")
      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 100"), Nil)
      sql("insert into table parquet_test select * from t where key = 100")
      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 100"),
        Row(100, "this is test 100") :: Nil)
      sql("drop oindex index1 on parquet_test")

      sql("insert overwrite table parquet_test select * from t where key > 100")
      sql("create oindex index1 on parquet_test (a)")
      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 100"), Nil)
      sql("insert into table parquet_test select * from t where key = 100")
      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 100"),
        Row(100, "this is test 100") :: Nil)
    }
  }

  test("filtering by string with duplicate refresh") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("insert overwrite table parquet_test select * from t")
      sql("create oindex index1 on parquet_test (a) using btree")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 1"),
        Row(1, "this is test 1") :: Nil)

      sql("insert into table parquet_test select * from t")
      val check_path = new Path(currentPath)
      assert(check_path.getFileSystem(
        new Configuration()).globStatus(new Path(check_path, "*.index")).length == 2)

      sql("refresh oindex on parquet_test")
      assert(check_path.getFileSystem(
        new Configuration()).globStatus(new Path(check_path, "*.index")).length == 4)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 1"),
        Row(1, "this is test 1") :: Row(1, "this is test 1") :: Nil)

      sql("refresh oindex on parquet_test")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 1"),
        Row(1, "this is test 1") :: Row(1, "this is test 1") :: Nil)
    }
  }

  test("filtering with string type index") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("create oindex index1 on parquet_test (b)")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE b = 'this is test 1'"),
        Row(1, "this is test 1") :: Nil)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a > 1 AND a <= 3"),
        Row(2, "this is test 2") :: Row(3, "this is test 3") :: Nil)
    }
  }

  test("test parquet use in") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("create oindex index1 on parquet_test (b)")

      // will use b in (....)
      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "b in('this is test 1','this is test 2','this is test 4')"),
        Row(1, "this is test 1") :: Row(2, "this is test 2")
          :: Row(4, "this is test 4") :: Nil)
    }
  }

  test("test parquet use in StringFieldNotCastDouble") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"$i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("create oindex index1 on parquet_test (b)")

      // will use b in(1,2,4), values cast to string
      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "b in(1,2,4)"),
        Row(1, "1") :: Row(2, "2")
          :: Row(4, "4") :: Nil)
    }
  }

  test("test parquet use in IntFieldNotCastDouble") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"$i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("create oindex index1 on parquet_test (a)")

      // will use a in (20,30,40), value cast to int
      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "a=10 AND a in (20,30,40)"), Nil)
    }
  }

  test("test parquet query include in") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"$i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "index1"),
      TestIndex("parquet_test", "index2")) {
      sql("create oindex index1 on parquet_test (a)")
      sql("create oindex index2 on parquet_test (b)")

      // ((cast(a#12 as double) = 10.0) || a#12 IN (20,30)),
      // not support cast(a#12 as double) = 10.0
      // can not use index
      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "a=10 or a in (20,30)"),
        Row(10, "10") :: Row(20, "20")
          :: Row(30, "30") :: Nil)

      // not support by index
      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "b='10' or a in (20,30)"),
        Row(10, "10") :: Row(20, "20")
          :: Row(30, "30") :: Nil)

      // not support by index
      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "b='10' or (b = '20' and a in (10,20,30))"),
        Row(10, "10") :: Row(20, "20") :: Nil)

      // use index
      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "b='10' and b in (10,20,30)"),
        Row(10, "10") :: Nil)

      // use index
      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "b='10' or b in (10,20,30)"),
        Row(10, "10") :: Row(20, "20")
          :: Row(30, "30")  :: Nil)
    }
  }


  test("test parquet use between") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"$i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("create oindex index1 on parquet_test (a)")

      // (a#12 >= 10) && (a#12 <= 30)
      // use index
      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "a BETWEEN 10 AND 12"),
        Row(10, "10") :: Row(11, "11")
          :: Row(12, "12") :: Nil)
    }
  }

  test("test orc use in") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table orc_test select * from t")
    withIndex(TestIndex("orc_test", "index1")) {
      sql("create oindex index1 on orc_test (b)")

      checkAnswer(sql("SELECT * FROM orc_test WHERE " +
        "b in('this is test 1','this is test 2','this is test 4')"),
        Row(1, "this is test 1") :: Row(2, "this is test 2")
          :: Row(4, "this is test 4") :: Nil)
    }
  }

  test("test orc use in StringFieldNotCastDouble") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"$i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table orc_test select * from t")
    withIndex(TestIndex("orc_test", "index1")) {
      sql("create oindex index1 on orc_test (b)")

      checkAnswer(sql("SELECT * FROM orc_test WHERE " +
        "b in(1,2,4)"),
        Row(1, "1") :: Row(2, "2")
          :: Row(4, "4") :: Nil)
    }
  }

  test("test orc use in IntFieldNotCastDouble") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"$i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table orc_test select * from t")
    withIndex(TestIndex("orc_test", "index1")) {
      sql("create oindex index1 on orc_test (a)")

      checkAnswer(sql("SELECT * FROM orc_test WHERE " +
        "a=10 AND a in (20,30,40)"), Nil)
    }
  }

  test("test orc query include in") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"$i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table orc_test select * from t")
    withIndex(TestIndex("orc_test", "index1"),
      TestIndex("orc_test", "index2")) {
      sql("create oindex index1 on orc_test (a)")
      sql("create oindex index2 on orc_test (b)")

      checkAnswer(sql("SELECT * FROM orc_test WHERE " +
        "a=10 or a in (20,30)"),
        Row(10, "10") :: Row(20, "20")
          :: Row(30, "30") :: Nil)

      checkAnswer(sql("SELECT * FROM orc_test WHERE " +
        "b='10' or a in (20,30)"),
        Row(10, "10") :: Row(20, "20")
          :: Row(30, "30") :: Nil)

      checkAnswer(sql("SELECT * FROM orc_test WHERE " +
        "b='10' or (b = '20' and a in (10,20,30))"),
        Row(10, "10") :: Row(20, "20") :: Nil)

      checkAnswer(sql("SELECT * FROM orc_test WHERE " +
        "b='10' and b in (10,20,30)"),
        Row(10, "10") :: Nil)

      checkAnswer(sql("SELECT * FROM orc_test WHERE " +
        "b='10' or b in (10,20,30)"),
        Row(10, "10") :: Row(20, "20")
          :: Row(30, "30")  :: Nil)
    }
  }


  test("test orc use between") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"$i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table orc_test select * from t")
    withIndex(TestIndex("orc_test", "index1")) {
      sql("create oindex index1 on orc_test (a)")

      checkAnswer(sql("SELECT * FROM orc_test WHERE " +
        "a BETWEEN 10 AND 12"),
        Row(10, "10") :: Row(11, "11")
          :: Row(12, "12") :: Nil)
    }
  }


  test("test oap use in") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("create oindex index1 on parquet_test (b)")

      // will use b in (....)
      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "b in('this is test 1','this is test 2','this is test 4')"),
        Row(1, "this is test 1") :: Row(2, "this is test 2")
          :: Row(4, "this is test 4") :: Nil)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "b in('this is test 2','this is test 1','this is test 2')"),
        Row(1, "this is test 1") :: Row(2, "this is test 2") :: Nil)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "b in('this is test 1','this is test 2','this is test 1')"),
        Row(1, "this is test 1") :: Row(2, "this is test 2") :: Nil)
    }
  }


  test("test oap use in IntFieldNotCastDouble") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"$i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("create oindex index1 on parquet_test (a)")

      // will use a in (20,30,40), value cast to int
      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "a=10 AND a in (20,30,40)"), Nil)
    }
  }

  test("test oap query include in") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"$i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "index1"),
      TestIndex("parquet_test", "index2")) {
      sql("create oindex index1 on parquet_test (a)")
      sql("create oindex index2 on parquet_test (b)")

      // ((cast(a#12 as double) = 10.0) || a#12 IN (20,30)),
      // not support cast(a#12 as double) = 10.0
      // can not use index
      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "a=10 or a in (20,30)"),
        Row(10, "10") :: Row(20, "20")
          :: Row(30, "30") :: Nil)

      // use index
      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "b='10' and b in (10,20,30)"),
        Row(10, "10") :: Nil)

      // use index
      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "b='10' or b in (10,20,30)"),
        Row(10, "10") :: Row(20, "20")
          :: Row(30, "30")  :: Nil)

      // not support by index
      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "b='10' or a in (20,30)"),
        Row(10, "10") :: Row(20, "20")
          :: Row(30, "30") :: Nil)

      // not support by index
      checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
        "b='10' or (b = '20' and a in (10,20,30))"),
        Row(10, "10") :: Row(20, "20") :: Nil)
    }
  }

  test("test parquet inner join") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i / 10, s"$i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "index1")) {
      sql("create oindex index1 on parquet_test (a)")

      checkAnswer(sql("select t2.a, sum(t2.b) " +
        "from (select a from parquet_test where a = 3 ) t1 " +
        "inner join parquet_test t2 on t1.a = t2.a " +
        "group by t2.a"),
        Row(3, 3450.0) :: Nil)
    }
  }

  test("test orc inner join") {
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i / 10, s"$i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table orc_test select * from t")
    withIndex(TestIndex("orc_test", "index1")) {
      sql("create oindex index1 on orc_test (a)")

      checkAnswer(sql("select t2.a, sum(t2.b) " +
        "from (select a from orc_test where a = 3 ) t1 " +
        "inner join orc_test t2 on t1.a = t2.a " +
        "group by t2.a"),
        Row(3, 3450.0) :: Nil)
    }
  }

  test("filtering null key") {
    withIndex(TestIndex("parquet_test", "idx1")) {
      val rowRDD = spark.sparkContext.parallelize(1 to 100, 3).map { i =>
        if (i <= 5) Seq(null, s"this is row $i") else Seq(i, s"this is row $i")
      }.map(Row.fromSeq)
      val schema =
        StructType(
          StructField("a", IntegerType) ::
            StructField("c", StringType) :: Nil)
      val df = spark.createDataFrame(rowRDD, schema)
      df.createOrReplaceTempView("t")
      val nullKeyRows = (1 to 5).map(i => Row(null, s"this is row $i"))

      sql("insert overwrite table parquet_test select * from t")

      sql("create oindex idx1 on parquet_test (a)")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a is null"), nullKeyRows)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a > 11 AND a <= 13"),
        Row(12, "this is row 12") :: Row(13, "this is row 13") :: Nil)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a <= 2"), Nil)

      sql("insert into table parquet_test values(null, 'this is row 400')")
      sql("insert into table parquet_test values(null, 'this is row 500')")
      sql("refresh oindex on parquet_test")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a is null"),
        nullKeyRows :+ Row(null, "this is row 400") :+ Row(null, "this is row 500"))
    }
  }

  test("filtering null key in Parquet format") {
    withIndex(TestIndex("parquet_test", "idx1")) {
      val rowRDD = spark.sparkContext.parallelize(1 to 100, 3).map { i =>
        if (i <= 5) Seq(null, s"this is row $i") else Seq(i, s"this is row $i")
      }.map(Row.fromSeq)
      val schema =
        StructType(
          StructField("a", IntegerType) ::
            StructField("c", StringType) :: Nil)
      val df = spark.createDataFrame(rowRDD, schema)
      df.createOrReplaceTempView("t")
      val nullKeyRows = (1 to 5).map(i => Row(null, s"this is row $i"))

      sql("insert overwrite table parquet_test select * from t")

      sql("create oindex idx1 on parquet_test (a)")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a is null"), nullKeyRows)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a > 11 AND a <= 13"),
        Row(12, "this is row 12") :: Row(13, "this is row 13") :: Nil)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a <= 2"), Nil)

      sql("insert into table parquet_test values(null, 'this is row 400')")
      sql("insert into table parquet_test values(null, 'this is row 500')")
      sql("refresh oindex on parquet_test")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a is null"),
        nullKeyRows :+ Row(null, "this is row 400") :+ Row(null, "this is row 500"))
    }
  }

  test("filtering null key in orc format") {
    withIndex(TestIndex("orc_test", "idx1")) {
      val rowRDD = spark.sparkContext.parallelize(1 to 100, 3).map { i =>
        if (i <= 5) Seq(null, s"this is row $i") else Seq(i, s"this is row $i")
      }.map(Row.fromSeq)
      val schema =
        StructType(
          StructField("a", IntegerType) ::
            StructField("c", StringType) :: Nil)
      val df = spark.createDataFrame(rowRDD, schema)
      df.createOrReplaceTempView("t")
      val nullKeyRows = (1 to 5).map(i => Row(null, s"this is row $i"))

      sql("insert overwrite table orc_test select * from t")

      sql("create oindex idx1 on orc_test (a)")

      checkAnswer(sql("SELECT * FROM orc_test WHERE a is null"), nullKeyRows)

      checkAnswer(sql("SELECT * FROM orc_test WHERE a > 11 AND a <= 13"),
        Row(12, "this is row 12") :: Row(13, "this is row 13") :: Nil)

      checkAnswer(sql("SELECT * FROM orc_test WHERE a <= 2"), Nil)

      sql("insert into table orc_test values(null, 'this is row 400')")
      sql("insert into table orc_test values(null, 'this is row 500')")
      sql("refresh oindex on orc_test")

      checkAnswer(sql("SELECT * FROM orc_test WHERE a is null"),
        nullKeyRows :+ Row(null, "this is row 400") :+ Row(null, "this is row 500"))
    }
  }

  test("filtering non-null key") {
    val rowRDD = spark.sparkContext.parallelize(1 to 10).map { i =>
      if (i <= 3) Seq(null, s"this is row $i") else Seq(i, s"this is row $i")
    }.map(Row.fromSeq)
    val schema =
      StructType(
        StructField("a", IntegerType) ::
          StructField("c", StringType) :: Nil)
    val df = spark.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("t")
    val nonNullKeyRows = (4 to 10).map(i => Row(i, s"this is row $i"))

    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "idx1")) {
      sql("create oindex idx1 on parquet_test (a)")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a is not null"), nonNullKeyRows)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a > 7 AND a <= 9"),
        Row(8, "this is row 8") :: Row(9, "this is row 9") :: Nil)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a <= 2"), Nil)
    }
  }

  test("filtering non-null key in Parquet format") {
    val rowRDD = spark.sparkContext.parallelize(1 to 10).map { i =>
      if (i <= 3) Seq(null, s"this is row $i") else Seq(i, s"this is row $i")
    }.map(Row.fromSeq)
    val schema =
      StructType(
        StructField("a", IntegerType) ::
          StructField("c", StringType) :: Nil)
    val df = spark.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("t")
    val nonNullKeyRows = (4 to 10).map(i => Row(i, s"this is row $i"))

    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "idx1")) {
      sql("create oindex idx1 on parquet_test (a)")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a is not null"), nonNullKeyRows)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a > 7 AND a <= 9"),
        Row(8, "this is row 8") :: Row(9, "this is row 9") :: Nil)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a <= 2"), Nil)
    }
  }

  test("filtering non-null key in orc format") {
    val rowRDD = spark.sparkContext.parallelize(1 to 10).map { i =>
      if (i <= 3) Seq(null, s"this is row $i") else Seq(i, s"this is row $i")
    }.map(Row.fromSeq)
    val schema =
      StructType(
        StructField("a", IntegerType) ::
          StructField("c", StringType) :: Nil)
    val df = spark.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("t")
    val nonNullKeyRows = (4 to 10).map(i => Row(i, s"this is row $i"))

    sql("insert overwrite table orc_test select * from t")
    withIndex(TestIndex("orc_test", "idx1")) {
      sql("create oindex idx1 on orc_test (a)")

      checkAnswer(sql("SELECT * FROM orc_test WHERE a is not null"), nonNullKeyRows)

      checkAnswer(sql("SELECT * FROM orc_test WHERE a > 7 AND a <= 9"),
        Row(8, "this is row 8") :: Row(9, "this is row 9") :: Nil)

      checkAnswer(sql("SELECT * FROM orc_test WHERE a <= 2"), Nil)
    }
  }

  test("filtering null key and normal key mixed") {
    val rowRDD = spark.sparkContext.parallelize(1 to 100, 3).map { i =>
      if (i <= 5) Seq(null, s"this is row $i") else Seq(i, s"this is row $i")
    }.map(Row.fromSeq)
    val schema =
      StructType(
        StructField("a", IntegerType) ::
          StructField("c", StringType) :: Nil)
    val df = spark.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("t")
    val nullKeyRows = (1 to 5).map(i => Row(null, s"this is row $i"))

    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "idx1")) {
      sql("create oindex idx1 on parquet_test (a)")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a > 11 AND a <= 13 or a is null"),
        Row(12, "this is row 12") :: Row(13, "this is row 13") :: Nil ++ nullKeyRows)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a > 23 and a is null"), Nil)
    }
  }

  test("filtering null key and normal key mixed in Parquet format") {
    val rowRDD = spark.sparkContext.parallelize(1 to 100, 3).map { i =>
      if (i <= 5) Seq(null, s"this is row $i") else Seq(i, s"this is row $i")
    }.map(Row.fromSeq)
    val schema =
      StructType(
        StructField("a", IntegerType) ::
          StructField("c", StringType) :: Nil)
    val df = spark.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("t")
    val nullKeyRows = (1 to 5).map(i => Row(null, s"this is row $i"))

    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "idx1")) {
      sql("create oindex idx1 on parquet_test (a)")

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a > 11 AND a <= 13 or a is null"),
        Row(12, "this is row 12") :: Row(13, "this is row 13") :: Nil ++ nullKeyRows)

      checkAnswer(sql("SELECT * FROM parquet_test WHERE a > 23 and a is null"), Nil)
    }
  }

  test("filtering null key and normal key mixed in orc format") {
    val rowRDD = spark.sparkContext.parallelize(1 to 100, 3).map { i =>
      if (i <= 5) Seq(null, s"this is row $i") else Seq(i, s"this is row $i")
    }.map(Row.fromSeq)
    val schema =
      StructType(
        StructField("a", IntegerType) ::
          StructField("c", StringType) :: Nil)
    val df = spark.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("t")
    val nullKeyRows = (1 to 5).map(i => Row(null, s"this is row $i"))

    sql("insert overwrite table orc_test select * from t")
    withIndex(TestIndex("orc_test", "idx1")) {
      sql("create oindex idx1 on orc_test (a)")

      checkAnswer(sql("SELECT * FROM orc_test WHERE a > 11 AND a <= 13 or a is null"),
        Row(12, "this is row 12") :: Row(13, "this is row 13") :: Nil ++ nullKeyRows)

      checkAnswer(sql("SELECT * FROM orc_test WHERE a > 23 and a is null"), Nil)
    }
  }

  test("get columns hit index") {
    val rowRDD = spark.sparkContext.parallelize(1 to 100, 3).map(i =>
      Seq(i, s"this is row $i")).map(Row.fromSeq)
    val schema =
      StructType(
        StructField("a", IntegerType) ::
          StructField("c", StringType) :: Nil)
    val df = spark.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("t")

    sql("insert overwrite table parquet_test select * from t")
    withIndex(
      TestIndex("parquet_test", "idx1"),
      TestIndex("parquet_test", "idx2")) {
      sql("create oindex idx1 on parquet_test (a)")
      sql("create oindex idx2 on parquet_test (a, b)")

      val df1 = sql("SELECT * FROM parquet_test WHERE a = 1")
      checkAnswer(df1, Row(1, "this is row 1") :: Nil)
      val ret1 = getColumnsHitIndex(df1.queryExecution.sparkPlan)
      assert(ret1.keySet.size == 1 && ret1.keySet.head == "a")
      assert(ret1.values.head.toString == BTreeIndex(BTreeIndexEntry(0)::Nil).toString)

      val df2 = sql("SELECT * FROM parquet_test WHERE a = 1 AND b = 'this is row 1'")
      checkAnswer(df2, Row(1, "this is row 1") :: Nil)
      val ret2 = getColumnsHitIndex(df2.queryExecution.sparkPlan)
      assert(ret2.keySet.size == 2 && ret2.contains("a") && ret2.contains("b"))
      assert(ret2("a") == ret2("b") && ret2("a").toString
        == BTreeIndex(BTreeIndexEntry(0)::BTreeIndexEntry(1)::Nil).toString)
    }
  }

  test("OAP-930 test columns hit index when in optimized to inset") {
    val data: Seq[(Int, String)] = (0 to 100).map { i => (i, s"this is row $i") }
    data.toDF("a", "b").createOrReplaceTempView("t")

    sql("insert overwrite table parquet_test select * from t")
    withIndex(TestIndex("parquet_test", "idx1")) {
      sql("create oindex idx1 on parquet_test (a)")
      // create sequence with one more element than in-to-inSet optimized threshold
      val intSeq = 0 to sqlContext.conf.optimizerInSetConversionThreshold
      val df = sql(s"SELECT * from parquet_test WHERE a in(" +
        s"${intSeq.map(_.toString).mkString(",")})")
      val rowList = intSeq.map(i => Row(i, s"this is row $i"))
      checkAnswer(df, rowList)
      val ret = getColumnsHitIndex(df.queryExecution.sparkPlan)
      assert(ret.keySet.size == 1 && ret.contains("a"))
    }
  }

  test("filtering parquet in FiberCache") {
    withSQLConf("spark.sql.oap.parquet.data.cache.enabled" -> "true") {
      val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
      data.toDF("key", "value").createOrReplaceTempView("t")
      sql("insert overwrite table parquet_test select * from t")
      withIndex(TestIndex("parquet_test", "index1")) {
        sql("create oindex index1 on parquet_test (a)")
        checkAnswer(sql("SELECT * FROM parquet_test WHERE a = 1"),
          Row(1, "this is test 1") :: Nil)
        checkAnswer(sql("SELECT * FROM parquet_test WHERE a > 1 AND a <= 3"),
          Row(2, "this is test 2") :: Row(3, "this is test 3") :: Nil)
      }
    }
  }

//  test("filtering orc in FiberCache") {
//    withSQLConf(OapConf.OAP_ORC_DATA_CACHE_ENABLED.key -> "true",
//      SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
//      val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
//      data.toDF("key", "value").createOrReplaceTempView("t")
//      sql("insert overwrite table orc_test select * from t")
//      withIndex(TestIndex("orc_test", "index1")) {
//        sql("create oindex index1 on orc_test (a)")
//        checkAnswer(sql("SELECT * FROM orc_test WHERE a = 1"),
//          Row(1, "this is test 1") :: Nil)
//        checkAnswer(sql("SELECT * FROM orc_test WHERE a > 1 AND a <= 3"),
//          Row(2, "this is test 2") :: Row(3, "this is test 3") :: Nil)
//      }
//    }
//  }

  test("filtering parquet in FiberCache with partition") {
    withSQLConf("spark.sql.oap.parquet.data.cache.enabled" -> "true") {
      val data: Seq[(Int, Int)] = (1 to 100).map { i => (i, i) }
      data.toDF("key", "value").createOrReplaceTempView("t")
      sql(
        """
          |INSERT OVERWRITE TABLE t_refresh_parquet
          |partition (b=1)
          |SELECT key from t where value < 4
        """.stripMargin)
      withIndex(TestIndex("t_refresh_parquet", "index1")) {
        sql("create oindex index1 on t_refresh_parquet (a)")
        checkAnswer(sql("select * from t_refresh_parquet where b = '1' and a = 2"),
          Row(2, 1) :: Nil)
      }
    }
  }

  test("filtering parquet without both code gen and off-heap cache") {
    spark.sqlContext.setConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
    withSQLConf("spark.sql.oap.parquet.data.cache.enabled" -> "false") {
      val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
      data.toDF("key", "value").createOrReplaceTempView("t")
      sql("insert overwrite table parquet_test select * from t")
      withIndex(TestIndex("parquet_test", "index1")) {
        sql("create oindex index1 on parquet_test (b)")

        // will use b in (....)
        checkAnswer(sql("SELECT * FROM parquet_test WHERE " +
          "b in('this is test 1','this is test 2','this is test 4')"),
          Row(1, "this is test 1") :: Row(2, "this is test 2")
            :: Row(4, "this is test 4") :: Nil)
      }
    }
    spark.sqlContext.setConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
  }

  test("filtering orc with off-heap column vector") {
    spark.sqlContext.setConf(SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key, "true")
    val data: Seq[(Int, String)] = (1 to 300).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table orc_test select * from t")
    withIndex(TestIndex("orc_test", "index1")) {
      sql("create oindex index1 on orc_test (b)")

      checkAnswer(sql("SELECT * FROM orc_test WHERE " +
        "b in('this is test 1','this is test 2','this is test 4')"),
        Row(1, "this is test 1") :: Row(2, "this is test 2")
          :: Row(4, "this is test 4") :: Nil)
    }
    spark.sqlContext.setConf(SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key, "false")
  }

  test("OAP-764 BloomFilterStatisticsReader#analyse unexpected return SkipFile") {
    // dig holes
    val data: Seq[(Int, String)] = (1 to 300).map { i => {
      if (i % 13 == 0) {
        (-1, "-1")
      } else {
        (i, s"this is test $i")
      }
    }}

    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table parquet_test select * from t")
    withSQLConf(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.key -> "true") {
      withIndex(TestIndex("parquet_test", "index1")) {
        sql("create oindex index1 on parquet_test (a)")
        // 13, 26, 39, 52 not in this file
        checkAnswer(sql("SELECT * FROM parquet_test WHERE a in (1, 13, 26, 39, 52)"),
          Row(1, "this is test 1"):: Nil)
      }
    }
  }
}
