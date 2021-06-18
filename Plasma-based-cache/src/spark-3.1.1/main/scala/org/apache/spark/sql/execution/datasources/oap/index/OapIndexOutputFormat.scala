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

package org.apache.spark.sql.execution.datasources.oap.index

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.parquet.format.CompressionCodec
import org.apache.parquet.hadoop.util.ContextUtil

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.index.OapIndexProperties.IndexVersion
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.types.StructType

private[index] class OapIndexOutputFormat extends FileOutputFormat[Void, InternalRow] {

  private val BTREE_WRITER_VERSION = OapConf.OAP_INDEX_BTREE_WRITER_VERSION.key

  private def getCodec(taskAttemptContext: TaskAttemptContext): CompressionCodec = {
    val configuration = ContextUtil.getConfiguration(taskAttemptContext)
    CompressionCodec.valueOf(
      configuration.get(
        OapConf.OAP_INDEX_BTREE_COMPRESSION.key,
        OapConf.OAP_INDEX_BTREE_COMPRESSION.defaultValueString).toUpperCase)
  }

  private def getWriterVersion(taskAttemptContext: TaskAttemptContext) = {
    val configuration = ContextUtil.getConfiguration(taskAttemptContext)
    val indexVersion =
      configuration.get(BTREE_WRITER_VERSION, OapIndexProperties.DEFAULT_WRITER_VERSION.toString)
    IndexVersion.fromString(indexVersion)
  }

  override def getRecordWriter(
      taskAttemptContext: TaskAttemptContext): RecordWriter[Void, InternalRow] = {

    val configuration = ContextUtil.getConfiguration(taskAttemptContext)

    def canBeSkipped(file: Path): Boolean = {
      val isAppend = configuration.get(OapIndexFileFormat.IS_APPEND).toBoolean
      if (isAppend) {
        val target = new Path(FileOutputFormat.getOutputPath(taskAttemptContext), file.getName)
        target.getFileSystem(configuration).exists(target)
      } else {
        false
      }
    }

    val codec = getCodec(taskAttemptContext)
    val writerVersion = getWriterVersion(taskAttemptContext)

    val extension = "." + configuration.get(OapIndexFileFormat.INDEX_TIME) +
        "." + configuration.get(OapIndexFileFormat.INDEX_NAME) +
        ".index"

    val file = getDefaultWorkFile(taskAttemptContext, extension)

    val schema = StructType.fromString(configuration.get(OapIndexFileFormat.ROW_SCHEMA))

    val indexType = configuration.get(OapIndexFileFormat.INDEX_TYPE, "")

    if (canBeSkipped(file)) {
      new DummyIndexRecordWriter()
    } else if (indexType == "BTREE") {
      BTreeIndexRecordWriter(configuration, file, schema, codec, writerVersion)
    } else if (indexType == "BITMAP") {
      val writer = file.getFileSystem(configuration).create(file, true)
      new BitmapIndexRecordWriter(configuration, writer, schema)
    } else {
      throw new OapException("Unknown Index Type: " + indexType)
    }
  }
}
