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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SortDirection, UnsafeRow}
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.statistics.StatsAnalysisResult
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

private[oap] object IndexScanner {
  val DUMMY_KEY_START: Key = new UnsafeRow() // we compare the ref not the value
  val DUMMY_KEY_END: Key = new UnsafeRow() // we compare the ref not the value
}

private[oap] abstract class IndexScanner(idxMeta: IndexMeta)
  extends Iterator[Int] with Serializable with Logging {

  def totalRows(): Long

  var intervalArray: ArrayBuffer[RangeInterval] = _

  protected var keySchema: StructType = _

  def keyNames: Seq[String] = keySchema.fieldNames

  /**
   * Scan N items from each index entry.
   */
  private var _internalLimit : Int = 0

  // _internalLimit setter
  def internalLimit_= (scanNum : Int) : Unit = _internalLimit = scanNum

  // _internalLimit getter
  def internalLimit : Int = _internalLimit

  def indexEntryScanIsLimited() : Boolean = _internalLimit > 0

  def meta: IndexMeta = idxMeta

  def getSchema: StructType = keySchema

  /**
   * Executor's analysis result by policies(include conf & statistics)
   * Process:
   *  1. See OAP_ENABLE_OINDEX
   *  2. See OAP_EXECUTOR_INDEX_SELECTION(allow to check by later policies)
   *  3. Compare Index file size / data file size ratio with OAP_INDEX_FILE_SIZE_MAX_RATIO
   *  4. Statistics(Min_Max, BloomFilter, Sample, Part_By_Value) by calling analysisResByStatistics
   *
   * @return FULL_SCAN / SKIP_INDEX(skip file by index)/ USE_INDEX / StatsAnalysisResult(coverage)
   *         -> letting upper level decide due to it sees the whole pictures of each IndexScanner's
   *         result.
   */
  def analysisResByPolicies(dataPath: Path, conf: Configuration): StatsAnalysisResult = {
    val indexPath = IndexUtils.getIndexFilePath(
      conf, dataPath, meta.name, meta.time)
    if (!indexPath.getFileSystem(conf).exists(indexPath)) {
      logDebug("No index file exist for data file: " + dataPath)
      StatsAnalysisResult.FULL_SCAN
    } else {
      val enableIndex = conf.getBoolean(OapConf.OAP_ENABLE_OINDEX.key,
        OapConf.OAP_ENABLE_OINDEX.defaultValue.get)
      if (!enableIndex) {
        StatsAnalysisResult.FULL_SCAN
      } else {
        val enableIndexSelection = conf.getBoolean(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.key,
          OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.defaultValue.get)
        if (!enableIndexSelection) {
          // Index selection is disabled, executor always uses the index
          StatsAnalysisResult.USE_INDEX
        } else {
          // Not blindly use the index, determining by more policies
          analysisResByStatistics(indexPath, dataPath, conf)
        }
      }
    }
  }

  // Decide by size ratio(generalized statistics : )) & statistics in index file
  private def analysisResByStatistics(indexPath: Path, dataPath: Path, conf: Configuration)
    : StatsAnalysisResult = {
    val fs = dataPath.getFileSystem(conf)
    require(fs.isFile(indexPath), s"Index file path $indexPath is a directory, it should be a file")

    // Policy 3: index file size < data file size)

    val filePolicyEnable =
      conf.getBoolean(OapConf.OAP_EXECUTOR_INDEX_SELECTION_FILE_POLICY.key,
        OapConf.OAP_EXECUTOR_INDEX_SELECTION_FILE_POLICY.defaultValue.get)

    val indexFileSize = fs.getFileStatus(indexPath).getLen
    val dataFileSize = fs.getFileStatus(dataPath).getLen
    val ratio = conf.getDouble(OapConf.OAP_INDEX_FILE_SIZE_MAX_RATIO.key,
      OapConf.OAP_INDEX_FILE_SIZE_MAX_RATIO.defaultValue.get)

    if (filePolicyEnable && indexFileSize > dataFileSize * ratio) {
      StatsAnalysisResult.FULL_SCAN
    } else {
      val statsPolicyEnable =
        conf.getBoolean(OapConf.OAP_EXECUTOR_INDEX_SELECTION_STATISTICS_POLICY.key,
          OapConf.OAP_EXECUTOR_INDEX_SELECTION_STATISTICS_POLICY.defaultValue.get)

      // Policy 4: statistics tells the scan cost
      if (statsPolicyEnable && !intervalArray.exists(_.isPrefixMatch)) {
        if (intervalArray.isEmpty) {
          StatsAnalysisResult.SKIP_INDEX
        } else {
          analyzeStatistics(indexPath, conf)
        }
      } else {
        StatsAnalysisResult.USE_INDEX
      }
      // More Policies
    }
  }

  /**
   * Judging if we should bypass this datafile or full scan or by index through statistics from
   * related index file,
   * return -1 means bypass, close to 1 means full scan and close to 0 means by index.
   */
  protected def analyzeStatistics(indexPath: Path, conf: Configuration): StatsAnalysisResult = {
    StatsAnalysisResult.USE_INDEX
  }

  def withKeySchema(schema: StructType): IndexScanner = {
    this.keySchema = schema
    this
  }

  def initialize(dataPath: Path, conf: Configuration): IndexScanner
}

// The building of Search Scanner according to the filter and indices,
private[oap] object ScannerBuilder extends Logging {

  type IntervalArrayMap = mutable.HashMap[String, ArrayBuffer[RangeInterval]]

  def combineIntervalMaps(
      leftMap: IntervalArrayMap,
      rightMap: IntervalArrayMap,
      ic: IndexContext,
      needMerge: Boolean): IntervalArrayMap = {
    for ((attribute, intervals) <- rightMap) {
      if (leftMap.contains(attribute)) {
        attribute match {
          case ic (filterOptimizer) => // extract the corresponding scannerBuilder
            // combine all intervals of the same attribute of leftMap and rightMap
            if (needMerge) {
              leftMap.put(attribute,
                filterOptimizer.mergeBound(leftMap.getOrElseUpdate(attribute, null), intervals))
            } else {
              // add bound of the same attribute to the left map
              leftMap.put(attribute,
                filterOptimizer.addBound(leftMap.getOrElse(attribute, null), intervals))
            }
          case _ => // this attribute does not exist, do nothing
        }
      } else {
        leftMap.put(attribute, intervals)
      }
    }
    leftMap
  }

  def optimizeFilterBound(filter: Filter, ic: IndexContext): IntervalArrayMap = {
    filter match {
      case And(leftFilter, rightFilter) =>
        val leftMap = optimizeFilterBound(leftFilter, ic)
        val rightMap = optimizeFilterBound(rightFilter, ic)
        combineIntervalMaps(leftMap, rightMap, ic, needMerge = true)
      case Or(leftFilter, rightFilter) =>
        val leftMap = optimizeFilterBound(leftFilter, ic)
        val rightMap = optimizeFilterBound(rightFilter, ic)
        combineIntervalMaps(leftMap, rightMap, ic, needMerge = false)
      case In(attribute, ic(keys)) =>
        val eqBounds = keys.distinct
          .map(key => RangeInterval(key, key, includeStart = true, includeEnd = true))
          .to[ArrayBuffer]
        mutable.HashMap(attribute -> eqBounds)
      case EqualTo(attribute, ic(key)) =>
        val ranger = RangeInterval(key, key, includeStart = true, includeEnd = true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case GreaterThanOrEqual(attribute, ic(key)) =>
        val ranger =
          RangeInterval(
            key,
            IndexScanner.DUMMY_KEY_END,
            includeStart = true,
            includeEnd = true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case GreaterThan(attribute, ic(key)) =>
        val ranger =
          RangeInterval(
            key,
            IndexScanner.DUMMY_KEY_END,
            includeStart = false,
            includeEnd = true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case LessThanOrEqual(attribute, ic(key)) =>
        val ranger =
          RangeInterval(
            IndexScanner.DUMMY_KEY_START,
            key,
            includeStart = true,
            includeEnd = true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case LessThan(attribute, ic(key)) =>
        val ranger =
          RangeInterval(
            IndexScanner.DUMMY_KEY_START,
            key,
            includeStart = true,
            includeEnd = false)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case IsNotNull(attribute) =>
        val ranger =
          RangeInterval(
            IndexScanner.DUMMY_KEY_START,
            IndexScanner.DUMMY_KEY_END,
            includeStart = true,
            includeEnd = true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case IsNull(attribute) =>
        val ranger =
          RangeInterval(
            IndexScanner.DUMMY_KEY_START,
            IndexScanner.DUMMY_KEY_END,
            includeStart = true,
            includeEnd = true,
            isNull = true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case StringStartsWith(attribute, v) =>
        val ranger =
          RangeInterval(
            InternalRow.apply(UTF8String.fromString(v)),
            InternalRow.apply(UTF8String.fromString(v)),
            includeStart = true,
            includeEnd = true,
            ignoreTail = true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case _ => mutable.HashMap.empty
    }
  }

  // return whether a Filter predicate can be supported by our current work
  def canSupport(filter: Filter, ic: IndexContext): Boolean = {
    filter match {
      case EqualTo(ic(indexer), _) => true
      case GreaterThan(ic(indexer), _) => true
      case GreaterThanOrEqual(ic(indexer), _) => true
      case LessThan(ic(indexer), _) => true
      case LessThanOrEqual(ic(indexer), _) => true
      case Or(ic(indexer), _) => true
      case And(ic(indexer), _) => true
      case In(ic(indexer), _) => true
      case _ => false
    }
  }

  def build(
      filters: Array[Filter],
      ic: IndexContext,
      scannerOptions: Map[String, String] = Map.empty,
      maxChooseSize: Int = 1,
      indexDisableList: String = ""): Array[Filter] = {
    if (filters == null || filters.isEmpty) {
      return filters
    }
    logDebug("Transform filters into Intervals:")
    val intervalMapArray = filters.map(optimizeFilterBound(_, ic))
    // reduce multiple hashMap to one hashMap("AND" operation)
    val intervalMap = intervalMapArray.reduce(
      (leftMap, rightMap) =>
        if (leftMap == null || leftMap.isEmpty) {
          rightMap
        } else if (rightMap == null || rightMap.isEmpty) {
          leftMap
        } else {
          combineIntervalMaps(leftMap, rightMap, ic, needMerge = true)
        }
    )

    if (intervalMap.nonEmpty) {
      intervalMap.foreach(intervals =>
        logDebug("\t" + intervals._1 + ": " + intervals._2.mkString(" - ")))

      ic.buildScanners(intervalMap, scannerOptions, maxChooseSize, indexDisableList)
    }

    filters.filterNot(canSupport(_, ic))
  }

}

private[oap] class IndexScanners(val scanners: Seq[IndexScanner])
  extends Iterator[Int] with Serializable with Logging{

  private var actualUsedScanners: Seq[IndexScanner] = Seq.empty

  private var backendIter: Iterator[Int] = _

  // Either it directs us to skip this file(SKIP_INDEX) or use index(USE_INDEX)
  def isIndexFileBeneficial(dataPath: Path, conf: Configuration): Boolean = {
    val analysisResults = scanners.map(s => (s, s.analysisResByPolicies(dataPath, conf)))
    if (analysisResults.forall(_._2 == StatsAnalysisResult.FULL_SCAN)) {
      false
    } else {
      if (analysisResults.forall(_._2 != StatsAnalysisResult.SKIP_INDEX)) {
        // OAP#1031 we should filter FULL_SCAN when USE_INDEX scene
        // because of FULL_SCAN may come from index file not exists in this partition and
        // FULL_SCAN Scanner needn't initialize index data.
        actualUsedScanners =
          analysisResults.filter(_._2 != StatsAnalysisResult.FULL_SCAN).map(_._1)
      }
      true
    }
  }

  def order: SortDirection = actualUsedScanners.head.meta.indexType.indexOrder.head

  def initialize(dataPath: Path, conf: Configuration): IndexScanners = {
    backendIter = actualUsedScanners.length match {
      case 0 => Iterator.empty
      case 1 =>
        actualUsedScanners.head.initialize(dataPath, conf)
      case _ =>
        actualUsedScanners.par.foreach(_.initialize(dataPath, conf))
        actualUsedScanners.map(_.toSet)
          .reduce((left, right) => {
            if (left.isEmpty || right.isEmpty) {
              Set.empty
            } else {
              left.intersect(right)
            }
          }).iterator
    }
    this
  }

  override def hasNext: Boolean = backendIter.hasNext

  override def next(): Int = backendIter.next

  override def toString(): String = scanners.map(_.toString()).mkString("|")

  def totalRows(): Long = scanners.head.totalRows()

}
