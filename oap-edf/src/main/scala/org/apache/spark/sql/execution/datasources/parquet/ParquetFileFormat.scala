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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.IOException
import java.net.URI
import java.util.TimeZone

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.parquet.schema.MessageType

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, ReadOnlyColumnVectorV1}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils, Utils}

class ParquetFileFormat
  extends FileFormat
    with DataSourceRegister
    with Logging
    with Serializable {
  // Hold a reference to the (serializable) singleton instance of ParquetLogRedirector. This
  // ensures the ParquetLogRedirector class is initialized whether an instance of ParquetFileFormat
  // is constructed or deserialized. Do not heed the Scala compiler's warning about an unused field
  // here.
  private val parquetLogRedirector = ParquetLogRedirector.INSTANCE

  override def shortName(): String = "parquet"

  override def toString: String = "Parquet"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[ParquetFileFormat]

  override def prepareWrite(
    sparkSession: SparkSession,
    job: Job,
    options: Map[String, String],
    dataSchema: StructType): OutputWriterFactory = {
    val parquetOptions = new ParquetOptions(options, sparkSession.sessionState.conf)

    val conf = ContextUtil.getConfiguration(job)

    val committerClass =
      conf.getClass(
        SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key,
        classOf[ParquetOutputCommitter],
        classOf[OutputCommitter])

    if (conf.get(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key) == null) {
      logInfo("Using default output committer for Parquet: " +
        classOf[ParquetOutputCommitter].getCanonicalName)
    } else {
      logInfo("Using user defined output committer for Parquet: " + committerClass.getCanonicalName)
    }

    conf.setClass(
      SQLConf.OUTPUT_COMMITTER_CLASS.key,
      committerClass,
      classOf[OutputCommitter])

    // We're not really using `ParquetOutputFormat[Row]` for writing data here, because we override
    // it in `ParquetOutputWriter` to support appending and dynamic partitioning.  The reason why
    // we set it here is to setup the output committer class to `ParquetOutputCommitter`, which is
    // bundled with `ParquetOutputFormat[Row]`.
    job.setOutputFormatClass(classOf[ParquetOutputFormat[Row]])

    ParquetOutputFormat.setWriteSupportClass(job, classOf[ParquetWriteSupport])

    // This metadata is useful for keeping UDTs like Vector/Matrix.
    ParquetWriteSupport.setSchema(dataSchema, conf)

    // Sets flags for `ParquetWriteSupport`, which converts Catalyst schema to Parquet
    // schema and writes actual rows to Parquet files.
    conf.set(
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      sparkSession.sessionState.conf.writeLegacyParquetFormat.toString)

    conf.set(
      SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key,
      sparkSession.sessionState.conf.parquetOutputTimestampType.toString)

    // Sets compression scheme
    conf.set(ParquetOutputFormat.COMPRESSION, parquetOptions.compressionCodecClassName)

    // SPARK-15719: Disables writing Parquet summary files by default.
    if (conf.get(ParquetOutputFormat.JOB_SUMMARY_LEVEL) == null
      && conf.get(ParquetOutputFormat.ENABLE_JOB_SUMMARY) == null) {
      conf.setEnum(ParquetOutputFormat.JOB_SUMMARY_LEVEL, JobSummaryLevel.NONE)
    }

    if (ParquetOutputFormat.getJobSummaryLevel(conf) != JobSummaryLevel.NONE
      && !classOf[ParquetOutputCommitter].isAssignableFrom(committerClass)) {
      // output summary is requested, but the class is not a Parquet Committer
      logWarning(s"Committer $committerClass is not a ParquetOutputCommitter and cannot" +
        s" create job summaries. " +
        s"Set Parquet option ${ParquetOutputFormat.JOB_SUMMARY_LEVEL} to NONE.")
    }

    new OutputWriterFactory {
      // This OutputWriterFactory instance is deserialized when writing Parquet files on the
      // executor side without constructing or deserializing ParquetFileFormat. Therefore, we hold
      // another reference to ParquetLogRedirector.INSTANCE here to ensure the latter class is
      // initialized.
      private val parquetLogRedirector = ParquetLogRedirector.INSTANCE

      override def newInstance(
        path: String,
        dataSchema: StructType,
        context: TaskAttemptContext): OutputWriter = {
        new ParquetOutputWriter(path, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        CodecConfig.from(context).getCodec.getExtension + ".parquet"
      }
    }
  }

  override def inferSchema(
    sparkSession: SparkSession,
    parameters: Map[String, String],
    files: Seq[FileStatus]): Option[StructType] = {
    val parquetOptions = new ParquetOptions(parameters, sparkSession.sessionState.conf)

    // Should we merge schemas from all Parquet part-files?
    val shouldMergeSchemas = parquetOptions.mergeSchema

    val mergeRespectSummaries = sparkSession.sessionState.conf.isParquetSchemaRespectSummaries

    val filesByType = splitFiles(files)

    // Sees which file(s) we need to touch in order to figure out the schema.
    //
    // Always tries the summary files first if users don't require a merged schema.  In this case,
    // "_common_metadata" is more preferable than "_metadata" because it doesn't contain row
    // groups information, and could be much smaller for large Parquet files with lots of row
    // groups.  If no summary file is available, falls back to some random part-file.
    //
    // NOTE: Metadata stored in the summary files are merged from all part-files.  However, for
    // user defined key-value metadata (in which we store Spark SQL schema), Parquet doesn't know
    // how to merge them correctly if some key is associated with different values in different
    // part-files.  When this happens, Parquet simply gives up generating the summary file.  This
    // implies that if a summary file presents, then:
    //
    //   1. Either all part-files have exactly the same Spark SQL schema, or
    //   2. Some part-files don't contain Spark SQL schema in the key-value metadata at all (thus
    //      their schemas may differ from each other).
    //
    // Here we tend to be pessimistic and take the second case into account.  Basically this means
    // we can't trust the summary files if users require a merged schema, and must touch all part-
    // files to do the merge.
    val filesToTouch =
    if (shouldMergeSchemas) {
      // Also includes summary files, 'cause there might be empty partition directories.

      // If mergeRespectSummaries config is true, we assume that all part-files are the same for
      // their schema with summary files, so we ignore them when merging schema.
      // If the config is disabled, which is the default setting, we merge all part-files.
      // In this mode, we only need to merge schemas contained in all those summary files.
      // You should enable this configuration only if you are very sure that for the parquet
      // part-files to read there are corresponding summary files containing correct schema.

      // As filed in SPARK-11500, the order of files to touch is a matter, which might affect
      // the ordering of the output columns. There are several things to mention here.
      //
      //  1. If mergeRespectSummaries config is false, then it merges schemas by reducing from
      //     the first part-file so that the columns of the lexicographically first file show
      //     first.
      //
      //  2. If mergeRespectSummaries config is true, then there should be, at least,
      //     "_metadata"s for all given files, so that we can ensure the columns of
      //     the lexicographically first file show first.
      //
      //  3. If shouldMergeSchemas is false, but when multiple files are given, there is
      //     no guarantee of the output order, since there might not be a summary file for the
      //     lexicographically first file, which ends up putting ahead the columns of
      //     the other files. However, this should be okay since not enabling
      //     shouldMergeSchemas means (assumes) all the files have the same schemas.

      val needMerged: Seq[FileStatus] =
        if (mergeRespectSummaries) {
          Seq.empty
        } else {
          filesByType.data
        }
      needMerged ++ filesByType.metadata ++ filesByType.commonMetadata
    } else {
      // Tries any "_common_metadata" first. Parquet files written by old versions or Parquet
      // don't have this.
      filesByType.commonMetadata.headOption
        // Falls back to "_metadata"
        .orElse(filesByType.metadata.headOption)
        // Summary file(s) not found, the Parquet file is either corrupted, or different part-
        // files contain conflicting user defined metadata (two or more values are associated
        // with a same key in different files).  In either case, we fall back to any of the
        // first part-file, and just assume all schemas are consistent.
        .orElse(filesByType.data.headOption)
        .toSeq
    }
    ParquetFileFormat.mergeSchemasInParallel(filesToTouch, sparkSession)
  }

  case class FileTypes(
    data: Seq[FileStatus],
    metadata: Seq[FileStatus],
    commonMetadata: Seq[FileStatus])

  private def splitFiles(allFiles: Seq[FileStatus]): FileTypes = {
    val leaves = allFiles.toArray.sortBy(_.getPath.toString)

    FileTypes(
      data = leaves.filterNot(f => isSummaryFile(f.getPath)),
      metadata =
        leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_METADATA_FILE),
      commonMetadata =
        leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE))
  }

  private def isSummaryFile(file: Path): Boolean = {
    file.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE ||
      file.getName == ParquetFileWriter.PARQUET_METADATA_FILE
  }

  /**
   * Returns whether the reader will return the rows as batch or not.
   */
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    val conf = sparkSession.sessionState.conf
    conf.parquetVectorizedReaderEnabled && conf.wholeStageEnabled &&
      schema.length <= conf.wholeStageMaxNumFields &&
      schema.forall(_.dataType.isInstanceOf[AtomicType])
  }

  override def vectorTypes(
    requiredSchema: StructType,
    partitionSchema: StructType,
    sqlConf: SQLConf): Option[Seq[String]] = {
    Option(Seq.fill(requiredSchema.fields.length + partitionSchema.fields.length)(
      if (sqlConf.getConf(SQLConf.PARQUET_DATASOURCE_CACHE_ENABLE)) {
        classOf[ReadOnlyColumnVectorV1].getName
      } else if (!sqlConf.offHeapColumnVectorEnabled) {
        classOf[OnHeapColumnVector].getName
      } else {
        classOf[OffHeapColumnVector].getName
      }
    ))
  }

  override def isSplitable(
    sparkSession: SparkSession,
    options: Map[String, String],
    path: Path): Boolean = {
    true
  }

  override def buildReaderWithPartitionValues(
    sparkSession: SparkSession,
    dataSchema: StructType,
    partitionSchema: StructType,
    requiredSchema: StructType,
    filters: Seq[Filter],
    options: Map[String, String],
    hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      requiredSchema.json)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      requiredSchema.json)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    ParquetWriteSupport.setSchema(requiredSchema, hadoopConf)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sparkSession.sessionState.conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp)

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    // TODO: if you move this into the closure it reverts to the default values.
    // If true, enable using the custom RecordReader for parquet. This only works for
    // a subset of the types (no complex types).
    val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
    val sqlConf = sparkSession.sessionState.conf
    val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
    val enableVectorizedReader: Boolean =
      sqlConf.parquetVectorizedReaderEnabled &&
        resultSchema.forall(_.dataType.isInstanceOf[AtomicType])
    val enableRecordFilter: Boolean = sqlConf.parquetRecordFilterEnabled
    val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
    val capacity = sqlConf.parquetVectorizedReaderBatchSize
    val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
    // Whole stage codegen (PhysicalRDD) is able to deal with batches directly
    val returningBatch = supportBatch(sparkSession, resultSchema)
    val pushDownDate = sqlConf.parquetFilterPushDownDate
    val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
    val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
    val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
    val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
    val isCaseSensitive = sqlConf.caseSensitiveAnalysis

    (file: PartitionedFile) => {
      assert(file.partitionValues.numFields == partitionSchema.size)

      val fileSplit =
        new FileSplit(new Path(new URI(file.filePath)), file.start, file.length, Array.empty)
      val filePath = fileSplit.getPath

      val split =
        new org.apache.parquet.hadoop.ParquetInputSplit(
          filePath,
          fileSplit.getStart,
          fileSplit.getStart + fileSplit.getLength,
          fileSplit.getLength,
          fileSplit.getLocations,
          null)

      val sharedConf = broadcastedHadoopConf.value.value

      lazy val footerFileMetaData =
        ParquetFileReader.readFooter(sharedConf, filePath, SKIP_ROW_GROUPS).getFileMetaData
      // Try to push down filters when filter push-down is enabled.
      val pushed = if (enableParquetFilterPushDown) {
        val parquetSchema = footerFileMetaData.getSchema
        val parquetFilters = new ParquetFilters(pushDownDate, pushDownTimestamp, pushDownDecimal,
          pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
        filters
          // Collects all converted Parquet filter predicates. Notice that not all predicates can be
          // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
          // is used here.
          .flatMap(parquetFilters.createFilter(parquetSchema, _))
          .reduceOption(FilterApi.and)
      } else {
        None
      }

      // PARQUET_INT96_TIMESTAMP_CONVERSION says to apply timezone conversions to int96 timestamps'
      // *only* if the file was created by something other than "parquet-mr", so check the actual
      // writer here for this file.  We have to do this per-file, as each file in the table may
      // have different writers.
      // Define isCreatedByParquetMr as function to avoid unnecessary parquet footer reads.
      def isCreatedByParquetMr: Boolean =
        footerFileMetaData.getCreatedBy().startsWith("parquet-mr")

      val convertTz =
        if (timestampConversion && !isCreatedByParquetMr) {
          Some(DateTimeUtils.getTimeZone(sharedConf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
        } else {
          None
        }

      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val hadoopAttemptContext =
        new TaskAttemptContextImpl(broadcastedHadoopConf.value.value, attemptId)

      // Try to push down filters when filter push-down is enabled.
      // Notice: This push-down is RowGroups level, not individual records.
      if (pushed.isDefined) {
        ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
      }
      val taskContext = Option(TaskContext.get())
      if (enableVectorizedReader) {
        val vectorizedReader = Utils.classForName(sharedConf.get(SQLConf.PARQUET_READER_IMPL.key))
          .getConstructor(classOf[TimeZone], java.lang.Boolean.TYPE, java.lang.Integer.TYPE)
          .newInstance(convertTz.orNull,
            (enableOffHeapColumnVector && taskContext.isDefined).asInstanceOf[java.lang.Boolean],
            capacity.asInstanceOf[java.lang.Integer])
          .asInstanceOf[VectorizedParquetRecordReader]
        val iter = new RecordReaderIterator(vectorizedReader)
        // SPARK-23457 Register a task completion lister before `initialization`.
        taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
        vectorizedReader.initialize(split, hadoopAttemptContext)
        logDebug(s"Appending $partitionSchema ${file.partitionValues}")
        vectorizedReader.initBatch(partitionSchema, file.partitionValues)
        if (returningBatch) {
          vectorizedReader.enableReturningBatches()
        }

        // UnsafeRowParquetRecordReader appends the columns internally to avoid another copy.
        iter.asInstanceOf[Iterator[InternalRow]]
      } else {
        logDebug(s"Falling back to parquet-mr")
        // ParquetRecordReader returns UnsafeRow
        val reader = if (pushed.isDefined && enableRecordFilter) {
          val parquetFilter = FilterCompat.get(pushed.get, null)
          new ParquetRecordReader[UnsafeRow](new ParquetReadSupport(convertTz), parquetFilter)
        } else {
          new ParquetRecordReader[UnsafeRow](new ParquetReadSupport(convertTz))
        }
        val iter = new RecordReaderIterator(reader)
        // SPARK-23457 Register a task completion lister before `initialization`.
        taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
        reader.initialize(split, hadoopAttemptContext)

        val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
        val joinedRow = new JoinedRow()
        val appendPartitionColumns = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

        // This is a horrible erasure hack...  if we type the iterator above, then it actually check
        // the type in next() and we get a class cast exception.  If we make that function return
        // Object, then we can defer the cast until later!
        if (partitionSchema.length == 0) {
          // There is no partition columns
          iter.asInstanceOf[Iterator[InternalRow]]
        } else {
          iter.asInstanceOf[Iterator[InternalRow]]
            .map(d => appendPartitionColumns(joinedRow(d, file.partitionValues)))
        }
      }
    }
  }

  override def supportDataType(dataType: DataType, isReadPath: Boolean): Boolean = dataType match {
    case _: AtomicType => true

    case st: StructType => st.forall { f => supportDataType(f.dataType, isReadPath) }

    case ArrayType(elementType, _) => supportDataType(elementType, isReadPath)

    case MapType(keyType, valueType, _) =>
      supportDataType(keyType, isReadPath) && supportDataType(valueType, isReadPath)

    case udt: UserDefinedType[_] => supportDataType(udt.sqlType, isReadPath)

    case _ => false
  }
}

object ParquetFileFormat extends Logging {
  private[parquet] def readSchema(
    footers: Seq[Footer], sparkSession: SparkSession): Option[StructType] = {

    val converter = new ParquetToSparkSchemaConverter(
      sparkSession.sessionState.conf.isParquetBinaryAsString,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp)

    val seen = mutable.HashSet[String]()
    val finalSchemas: Seq[StructType] = footers.flatMap { footer =>
      val metadata = footer.getParquetMetadata.getFileMetaData
      val serializedSchema = metadata
        .getKeyValueMetaData
        .asScala.toMap
        .get(ParquetReadSupport.SPARK_METADATA_KEY)
      if (serializedSchema.isEmpty) {
        // Falls back to Parquet schema if no Spark SQL schema found.
        Some(converter.convert(metadata.getSchema))
      } else if (!seen.contains(serializedSchema.get)) {
        seen += serializedSchema.get

        // Don't throw even if we failed to parse the serialized Spark schema. Just fallback to
        // whatever is available.
        Some(Try(DataType.fromJson(serializedSchema.get))
          .recover { case _: Throwable =>
            logInfo(
              "Serialized Spark schema in Parquet key-value metadata is not in JSON format, " +
                "falling back to the deprecated DataType.fromCaseClassString parser.")
            LegacyTypeStringParser.parse(serializedSchema.get)
          }
          .recover { case cause: Throwable =>
            logWarning(
              s"""Failed to parse serialized Spark schema in Parquet key-value metadata:
                 |\t$serializedSchema
               """.stripMargin,
              cause)
          }
          .map(_.asInstanceOf[StructType])
          .getOrElse {
            // Falls back to Parquet schema if Spark SQL schema can't be parsed.
            converter.convert(metadata.getSchema)
          })
      } else {
        None
      }
    }

    finalSchemas.reduceOption { (left, right) =>
      try left.merge(right) catch { case e: Throwable =>
        throw new SparkException(s"Failed to merge incompatible schemas $left and $right", e)
      }
    }
  }

  /**
   * Reads Parquet footers in multi-threaded manner.
   * If the config "spark.sql.files.ignoreCorruptFiles" is set to true, we will ignore the corrupted
   * files when reading footers.
   */
  private[parquet] def readParquetFootersInParallel(
    conf: Configuration,
    partFiles: Seq[FileStatus],
    ignoreCorruptFiles: Boolean): Seq[Footer] = {
    ThreadUtils.parmap(partFiles, "readingParquetFooters", 8) { currentFile =>
      try {
        // Skips row group information since we only need the schema.
        // ParquetFileReader.readFooter throws RuntimeException, instead of IOException,
        // when it can't read the footer.
        Some(new Footer(currentFile.getPath(),
          ParquetFileReader.readFooter(
            conf, currentFile, SKIP_ROW_GROUPS)))
      } catch { case e: RuntimeException =>
        if (ignoreCorruptFiles) {
          logWarning(s"Skipped the footer in the corrupted file: $currentFile", e)
          None
        } else {
          throw new IOException(s"Could not read footer for file: $currentFile", e)
        }
      }
    }.flatten
  }

  /**
   * Figures out a merged Parquet schema with a distributed Spark job.
   *
   * Note that locality is not taken into consideration here because:
   *
   *  1. For a single Parquet part-file, in most cases the footer only resides in the last block of
   *     that file.  Thus we only need to retrieve the location of the last block.  However, Hadoop
   *     `FileSystem` only provides API to retrieve locations of all blocks, which can be
   *     potentially expensive.
   *
   *  2. This optimization is mainly useful for S3, where file metadata operations can be pretty
   *     slow.  And basically locality is not available when using S3 (you can't run computation on
   *     S3 nodes).
   */
  def mergeSchemasInParallel(
    filesToTouch: Seq[FileStatus],
    sparkSession: SparkSession): Option[StructType] = {
    val assumeBinaryIsString = sparkSession.sessionState.conf.isParquetBinaryAsString
    val assumeInt96IsTimestamp = sparkSession.sessionState.conf.isParquetINT96AsTimestamp
    val serializedConf = new SerializableConfiguration(sparkSession.sessionState.newHadoopConf())

    // !! HACK ALERT !!
    //
    // Parquet requires `FileStatus`es to read footers.  Here we try to send cached `FileStatus`es
    // to executor side to avoid fetching them again.  However, `FileStatus` is not `Serializable`
    // but only `Writable`.  What makes it worse, for some reason, `FileStatus` doesn't play well
    // with `SerializableWritable[T]` and always causes a weird `IllegalStateException`.  These
    // facts virtually prevents us to serialize `FileStatus`es.
    //
    // Since Parquet only relies on path and length information of those `FileStatus`es to read
    // footers, here we just extract them (which can be easily serialized), send them to executor
    // side, and resemble fake `FileStatus`es there.
    val partialFileStatusInfo = filesToTouch.map(f => (f.getPath.toString, f.getLen))

    // Set the number of partitions to prevent following schema reads from generating many tasks
    // in case of a small number of parquet files.
    val numParallelism = Math.min(Math.max(partialFileStatusInfo.size, 1),
      sparkSession.sparkContext.defaultParallelism)

    val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles

    // Issues a Spark job to read Parquet schema in parallel.
    val partiallyMergedSchemas =
      sparkSession
        .sparkContext
        .parallelize(partialFileStatusInfo, numParallelism)
        .mapPartitions { iterator =>
          // Resembles fake `FileStatus`es with serialized path and length information.
          val fakeFileStatuses = iterator.map { case (path, length) =>
            new FileStatus(length, false, 0, 0, 0, 0, null, null, null, new Path(path))
          }.toSeq

          // Reads footers in multi-threaded manner within each task
          val footers =
            ParquetFileFormat.readParquetFootersInParallel(
              serializedConf.value, fakeFileStatuses, ignoreCorruptFiles)

          // Converter used to convert Parquet `MessageType` to Spark SQL `StructType`
          val converter = new ParquetToSparkSchemaConverter(
            assumeBinaryIsString = assumeBinaryIsString,
            assumeInt96IsTimestamp = assumeInt96IsTimestamp)
          if (footers.isEmpty) {
            Iterator.empty
          } else {
            var mergedSchema = ParquetFileFormat.readSchemaFromFooter(footers.head, converter)
            footers.tail.foreach { footer =>
              val schema = ParquetFileFormat.readSchemaFromFooter(footer, converter)
              try {
                mergedSchema = mergedSchema.merge(schema)
              } catch { case cause: SparkException =>
                throw new SparkException(
                  s"Failed merging schema of file ${footer.getFile}:\n${schema.treeString}", cause)
              }
            }
            Iterator.single(mergedSchema)
          }
        }.collect()

    if (partiallyMergedSchemas.isEmpty) {
      None
    } else {
      var finalSchema = partiallyMergedSchemas.head
      partiallyMergedSchemas.tail.foreach { schema =>
        try {
          finalSchema = finalSchema.merge(schema)
        } catch { case cause: SparkException =>
          throw new SparkException(
            s"Failed merging schema:\n${schema.treeString}", cause)
        }
      }
      Some(finalSchema)
    }
  }

  /**
   * Reads Spark SQL schema from a Parquet footer.  If a valid serialized Spark SQL schema string
   * can be found in the file metadata, returns the deserialized [[StructType]], otherwise, returns
   * a [[StructType]] converted from the [[MessageType]] stored in this footer.
   */
  def readSchemaFromFooter(
    footer: Footer, converter: ParquetToSparkSchemaConverter): StructType = {
    val fileMetaData = footer.getParquetMetadata.getFileMetaData
    fileMetaData
      .getKeyValueMetaData
      .asScala.toMap
      .get(ParquetReadSupport.SPARK_METADATA_KEY)
      .flatMap(deserializeSchemaString)
      .getOrElse(converter.convert(fileMetaData.getSchema))
  }

  private def deserializeSchemaString(schemaString: String): Option[StructType] = {
    // Tries to deserialize the schema string as JSON first, then falls back to the case class
    // string parser (data generated by older versions of Spark SQL uses this format).
    Try(DataType.fromJson(schemaString).asInstanceOf[StructType]).recover {
      case _: Throwable =>
        logInfo(
          "Serialized Spark schema in Parquet key-value metadata is not in JSON format, " +
            "falling back to the deprecated DataType.fromCaseClassString parser.")
        LegacyTypeStringParser.parse(schemaString).asInstanceOf[StructType]
    }.recoverWith {
      case cause: Throwable =>
        logWarning(
          "Failed to parse and ignored serialized Spark schema in " +
            s"Parquet key-value metadata:\n\t$schemaString", cause)
        Failure(cause)
    }.toOption
  }
}
