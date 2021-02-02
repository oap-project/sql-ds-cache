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

package org.apache.spark.sql.execution.datasources

import java.util.Locale

import scala.collection.mutable

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, QualifiedTableName}
import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoDir, InsertIntoStatement, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Replaces generic operations with specific variants that are designed to work with Spark
 * SQL Data Sources.
 *
 * Note that, this rule must be run after `PreprocessTableCreation` and
 * `PreprocessTableInsertion`.
 */
case class DataSourceAnalysis(conf: SQLConf) extends Rule[LogicalPlan] with CastSupport {

  def resolver: Resolver = conf.resolver

  // Visible for testing.
  def convertStaticPartitions(
      sourceAttributes: Seq[Attribute],
      providedPartitions: Map[String, Option[String]],
      targetAttributes: Seq[Attribute],
      targetPartitionSchema: StructType): Seq[NamedExpression] = {

    assert(providedPartitions.exists(_._2.isDefined))

    val staticPartitions = providedPartitions.flatMap {
      case (partKey, Some(partValue)) => (partKey, partValue) :: Nil
      case (_, None) => Nil
    }

    // The sum of the number of static partition columns and columns provided in the SELECT
    // clause needs to match the number of columns of the target table.
    if (staticPartitions.size + sourceAttributes.size != targetAttributes.size) {
      throw new AnalysisException(
        s"The data to be inserted needs to have the same number of " +
          s"columns as the target table: target table has ${targetAttributes.size} " +
          s"column(s) but the inserted data has ${sourceAttributes.size + staticPartitions.size} " +
          s"column(s), which contain ${staticPartitions.size} partition column(s) having " +
          s"assigned constant values.")
    }

    if (providedPartitions.size != targetPartitionSchema.fields.size) {
      throw new AnalysisException(
        s"The data to be inserted needs to have the same number of " +
          s"partition columns as the target table: target table " +
          s"has ${targetPartitionSchema.fields.size} partition column(s) but the inserted " +
          s"data has ${providedPartitions.size} partition columns specified.")
    }

    staticPartitions.foreach {
      case (partKey, partValue) =>
        if (!targetPartitionSchema.fields.exists(field => resolver(field.name, partKey))) {
          throw new AnalysisException(
            s"$partKey is not a partition column. Partition columns are " +
              s"${targetPartitionSchema.fields.map(_.name).mkString("[", ",", "]")}")
        }
    }

    val partitionList = targetPartitionSchema.fields.map { field =>
      val potentialSpecs = staticPartitions.filter {
        case (partKey, partValue) => resolver(field.name, partKey)
      }
      if (potentialSpecs.isEmpty) {
        None
      } else if (potentialSpecs.size == 1) {
        val partValue = potentialSpecs.head._2
        conf.storeAssignmentPolicy match {
          // SPARK-30844: try our best to follow StoreAssignmentPolicy for static partition
          // values but not completely follow because we can't do static type checking due to
          // the reason that the parser has erased the type info of static partition values
          // and converted them to string.
          case StoreAssignmentPolicy.ANSI | StoreAssignmentPolicy.STRICT =>
            Some(Alias(AnsiCast(Literal(partValue), field.dataType,
              Option(conf.sessionLocalTimeZone)), field.name)())
          case _ =>
            Some(Alias(cast(Literal(partValue), field.dataType), field.name)())
        }
      } else {
        throw new AnalysisException(
          s"Partition column ${field.name} have multiple values specified, " +
            s"${potentialSpecs.mkString("[", ", ", "]")}. Please only specify a single value.")
      }
    }

    // We first drop all leading static partitions using dropWhile and check if there is
    // any static partition appear after dynamic partitions.
    partitionList.dropWhile(_.isDefined).collectFirst {
      case Some(_) =>
        throw new AnalysisException(
          s"The ordering of partition columns is " +
            s"${targetPartitionSchema.fields.map(_.name).mkString("[", ",", "]")}. " +
            "All partition columns having constant values need to appear before other " +
            "partition columns that do not have an assigned constant value.")
    }

    assert(partitionList.take(staticPartitions.size).forall(_.isDefined))
    val projectList =
      sourceAttributes.take(targetAttributes.size - targetPartitionSchema.fields.size) ++
        partitionList.take(staticPartitions.size).map(_.get) ++
        sourceAttributes.takeRight(targetPartitionSchema.fields.size - staticPartitions.size)

    projectList
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case CreateTable(tableDesc, mode, None) if DDLUtils.isDatasourceTable(tableDesc) =>
      CreateDataSourceTableCommand(tableDesc, ignoreIfExists = mode == SaveMode.Ignore)

    case CreateTable(tableDesc, mode, Some(query))
      if query.resolved && DDLUtils.isDatasourceTable(tableDesc) =>
      CreateDataSourceTableAsSelectCommand(tableDesc, mode, query, query.output.map(_.name))

    case InsertIntoStatement(l @ LogicalRelation(_: InsertableRelation, _, _, _),
    parts, query, overwrite, false) if parts.isEmpty =>
      InsertIntoDataSourceCommand(l, query, overwrite)

    case InsertIntoDir(_, storage, provider, query, overwrite)
      if provider.isDefined && provider.get.toLowerCase(Locale.ROOT) != DDLUtils.HIVE_PROVIDER =>

      val outputPath = new Path(storage.locationUri.get)
      if (overwrite) DDLUtils.verifyNotReadPath(query, outputPath)

      InsertIntoDataSourceDirCommand(storage, provider.get, query, overwrite)

    case i @ InsertIntoStatement(
    l @ LogicalRelation(t: HadoopFsRelation, _, table, _), parts, query, overwrite, _) =>
      // If the InsertIntoTable command is for a partitioned HadoopFsRelation and
      // the user has specified static partitions, we add a Project operator on top of the query
      // to include those constant column values in the query result.
      //
      // Example:
      // Let's say that we have a table "t", which is created by
      // CREATE TABLE t (a INT, b INT, c INT) USING parquet PARTITIONED BY (b, c)
      // The statement of "INSERT INTO TABLE t PARTITION (b=2, c) SELECT 1, 3"
      // will be converted to "INSERT INTO TABLE t PARTITION (b, c) SELECT 1, 2, 3".
      //
      // Basically, we will put those partition columns having a assigned value back
      // to the SELECT clause. The output of the SELECT clause is organized as
      // normal_columns static_partitioning_columns dynamic_partitioning_columns.
      // static_partitioning_columns are partitioning columns having assigned
      // values in the PARTITION clause (e.g. b in the above example).
      // dynamic_partitioning_columns are partitioning columns that do not assigned
      // values in the PARTITION clause (e.g. c in the above example).
      val actualQuery = if (parts.exists(_._2.isDefined)) {
        val projectList = convertStaticPartitions(
          sourceAttributes = query.output,
          providedPartitions = parts,
          targetAttributes = l.output,
          targetPartitionSchema = t.partitionSchema)
        Project(projectList, query)
      } else {
        query
      }

      // Sanity check
      if (t.location.rootPaths.size != 1) {
        throw new AnalysisException("Can only write data to relations with a single path.")
      }

      val outputPath = t.location.rootPaths.head
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append

      val partitionSchema = actualQuery.resolve(
        t.partitionSchema, t.sparkSession.sessionState.analyzer.resolver)
      val staticPartitions = parts.filter(_._2.nonEmpty).map { case (k, v) => k -> v.get }

      val insertCommand = InsertIntoHadoopFsRelationCommand(
        outputPath,
        staticPartitions,
        i.ifPartitionNotExists,
        partitionSchema,
        t.bucketSpec,
        t.fileFormat,
        t.options,
        actualQuery,
        mode,
        table,
        Some(t.location),
        actualQuery.output.map(_.name))

      // For dynamic partition overwrite, we do not delete partition directories ahead.
      // We write to staging directories and move to final partition directories after writing
      // job is done. So it is ok to have outputPath try to overwrite inputpath.
      if (overwrite && !insertCommand.dynamicPartitionOverwrite) {
        DDLUtils.verifyNotReadPath(actualQuery, outputPath)
      }
      insertCommand
  }
}


/**
 * Replaces [[UnresolvedCatalogRelation]] with concrete relation logical plans.
 *
 * TODO: we should remove the special handling for hive tables after completely making hive as a
 * data source.
 */
class FindDataSourceTable(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  private def readDataSourceTable(table: CatalogTable): LogicalPlan = {
    val qualifiedTableName = QualifiedTableName(table.database, table.identifier.table)
    val catalog = sparkSession.sessionState.catalog
    catalog.getCachedPlan(qualifiedTableName, () => {
      val pathOption = table.storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
      val dataSource =
        DataSource(
          sparkSession,
          // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
          // inferred at runtime. We should still support it.
          userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
          partitionColumns = table.partitionColumnNames,
          bucketSpec = table.bucketSpec,
          className = table.provider.get,
          options = table.storage.properties ++ pathOption,
          catalogTable = Some(table))
      LogicalRelation(dataSource.resolveRelation(checkFilesExist = false), table)
    })
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case i @ InsertIntoStatement(UnresolvedCatalogRelation(tableMeta), _, _, _, _)
      if DDLUtils.isDatasourceTable(tableMeta) =>
      i.copy(table = readDataSourceTable(tableMeta))

    case i @ InsertIntoStatement(UnresolvedCatalogRelation(tableMeta), _, _, _, _) =>
      i.copy(table = DDLUtils.readHiveTable(tableMeta))

    case UnresolvedCatalogRelation(tableMeta) if DDLUtils.isDatasourceTable(tableMeta) =>
      readDataSourceTable(tableMeta)

    case UnresolvedCatalogRelation(tableMeta) =>
      DDLUtils.readHiveTable(tableMeta)
  }
}


/**
 * A Strategy for planning scans over data sources defined using the sources API.
 */
case class DataSourceStrategy(conf: SQLConf) extends Strategy with Logging with CastSupport {
  import DataSourceStrategy._

  def apply(plan: LogicalPlan): Seq[execution.SparkPlan] = plan match {
    case ScanOperation(projects, filters, l @ LogicalRelation(t: CatalystScan, _, _, _)) =>
      pruneFilterProjectRaw(
        l,
        projects,
        filters,
        (requestedColumns, allPredicates, _) =>
          toCatalystRDD(l, requestedColumns, t.buildScan(requestedColumns, allPredicates))) :: Nil

    case ScanOperation(projects, filters,
    l @ LogicalRelation(t: PrunedFilteredScan, _, _, _)) =>
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, f) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray, f))) :: Nil

    case ScanOperation(projects, filters, l @ LogicalRelation(t: PrunedScan, _, _, _)) =>
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, _) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray))) :: Nil

    case l @ LogicalRelation(baseRelation: TableScan, _, _, _) =>
      RowDataSourceScanExec(
        l.output,
        l.output.indices,
        Set.empty,
        Set.empty,
        toCatalystRDD(l, baseRelation.buildScan()),
        baseRelation,
        None) :: Nil

    case _ => Nil
  }

  // Based on Public API.
  private def pruneFilterProject(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      scanBuilder: (Seq[Attribute], Array[Filter]) => RDD[InternalRow]) = {
    pruneFilterProjectRaw(
      relation,
      projects,
      filterPredicates,
      (requestedColumns, _, pushedFilters) => {
        scanBuilder(requestedColumns, pushedFilters.toArray)
      })
  }

  // Based on Catalyst expressions. The `scanBuilder` function accepts three arguments:
  //
  //  1. A `Seq[Attribute]`, containing all required column attributes. Used to handle relation
  //     traits that support column pruning (e.g. `PrunedScan` and `PrunedFilteredScan`).
  //
  //  2. A `Seq[Expression]`, containing all gathered Catalyst filter expressions, only used for
  //     `CatalystScan`.
  //
  //  3. A `Seq[Filter]`, containing all data source `Filter`s that are converted from (possibly a
  //     subset of) Catalyst filter expressions and can be handled by `relation`.  Used to handle
  //     relation traits (`CatalystScan` excluded) that support filter push-down (e.g.
  //     `PrunedFilteredScan` and `HadoopFsRelation`).
  //
  // Note that 2 and 3 shouldn't be used together.
  private def pruneFilterProjectRaw(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      scanBuilder: (Seq[Attribute], Seq[Expression], Seq[Filter]) => RDD[InternalRow]): SparkPlan = {

    val projectSet = AttributeSet(projects.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

    val candidatePredicates = filterPredicates.map { _ transform {
      case a: AttributeReference => relation.attributeMap(a) // Match original case of attributes.
    }}

    val (unhandledPredicates, pushedFilters, handledFilters) =
      selectFilters(relation.relation, candidatePredicates)

    // Combines all Catalyst filter `Expression`s that are either not convertible to data source
    // `Filter`s or cannot be handled by `relation`.
    val filterCondition = unhandledPredicates.reduceLeftOption(expressions.And)

    if (projects.map(_.toAttribute) == projects &&
      projectSet.size == projects.size &&
      filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val requestedColumns = projects
        // Safe due to if above.
        .asInstanceOf[Seq[Attribute]]
        // Match original case of attributes.
        .map(relation.attributeMap)

      val scan = RowDataSourceScanExec(
        relation.output,
        requestedColumns.map(relation.output.indexOf),
        pushedFilters.toSet,
        handledFilters,
        scanBuilder(requestedColumns, candidatePredicates, pushedFilters),
        relation.relation,
        relation.catalogTable.map(_.identifier))
      filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan)
    } else {
      // A set of column attributes that are only referenced by pushed down filters.  We can
      // eliminate them from requested columns.
      val handledSet = {
        val handledPredicates = filterPredicates.filterNot(unhandledPredicates.contains)
        val unhandledSet = AttributeSet(unhandledPredicates.flatMap(_.references))
        AttributeSet(handledPredicates.flatMap(_.references)) --
          (projectSet ++ unhandledSet).map(relation.attributeMap)
      }
      // Don't request columns that are only referenced by pushed filters.
      val requestedColumns =
        (projectSet ++ filterSet -- handledSet).map(relation.attributeMap).toSeq

      val scan = RowDataSourceScanExec(
        relation.output,
        requestedColumns.map(relation.output.indexOf),
        pushedFilters.toSet,
        handledFilters,
        scanBuilder(requestedColumns, candidatePredicates, pushedFilters),
        relation.relation,
        relation.catalogTable.map(_.identifier))
      execution.ProjectExec(
        projects, filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan))
    }
  }

  /**
   * Convert RDD of Row into RDD of InternalRow with objects in catalyst types
   */
  private[this] def toCatalystRDD(
      relation: LogicalRelation,
      output: Seq[Attribute],
      rdd: RDD[Row]): RDD[InternalRow] = {
    DataSourceStrategy.toCatalystRDD(relation.relation, output, rdd)
  }

  /**
   * Convert RDD of Row into RDD of InternalRow with objects in catalyst types
   */
  private[this] def toCatalystRDD(relation: LogicalRelation, rdd: RDD[Row]): RDD[InternalRow] = {
    toCatalystRDD(relation, relation.output, rdd)
  }
}

object DataSourceStrategy {
  /**
   * The attribute name may differ from the one in the schema if the query analyzer
   * is case insensitive. We should change attribute names to match the ones in the schema,
   * so we do not need to worry about case sensitivity anymore.
   */
  protected[sql] def normalizeExprs(
      exprs: Seq[Expression],
      attributes: Seq[AttributeReference]): Seq[Expression] = {
    exprs.map { e =>
      e transform {
        case a: AttributeReference =>
          a.withName(attributes.find(_.semanticEquals(a)).getOrElse(a).name)
      }
    }
  }

  private def translateLeafNodeFilter(
      predicate: Expression,
      pushableColumn: PushableColumnBase): Option[Filter] = predicate match {
    case expressions.EqualTo(pushableColumn(name), Literal(v, t)) =>
      Some(sources.EqualTo(name, convertToScala(v, t)))
    case expressions.EqualTo(Literal(v, t), pushableColumn(name)) =>
      Some(sources.EqualTo(name, convertToScala(v, t)))

    case expressions.EqualNullSafe(pushableColumn(name), Literal(v, t)) =>
      Some(sources.EqualNullSafe(name, convertToScala(v, t)))
    case expressions.EqualNullSafe(Literal(v, t), pushableColumn(name)) =>
      Some(sources.EqualNullSafe(name, convertToScala(v, t)))

    case expressions.GreaterThan(pushableColumn(name), Literal(v, t)) =>
      Some(sources.GreaterThan(name, convertToScala(v, t)))
    case expressions.GreaterThan(Literal(v, t), pushableColumn(name)) =>
      Some(sources.LessThan(name, convertToScala(v, t)))

    case expressions.LessThan(pushableColumn(name), Literal(v, t)) =>
      Some(sources.LessThan(name, convertToScala(v, t)))
    case expressions.LessThan(Literal(v, t), pushableColumn(name)) =>
      Some(sources.GreaterThan(name, convertToScala(v, t)))

    case expressions.GreaterThanOrEqual(pushableColumn(name), Literal(v, t)) =>
      Some(sources.GreaterThanOrEqual(name, convertToScala(v, t)))
    case expressions.GreaterThanOrEqual(Literal(v, t), pushableColumn(name)) =>
      Some(sources.LessThanOrEqual(name, convertToScala(v, t)))

    case expressions.LessThanOrEqual(pushableColumn(name), Literal(v, t)) =>
      Some(sources.LessThanOrEqual(name, convertToScala(v, t)))
    case expressions.LessThanOrEqual(Literal(v, t), pushableColumn(name)) =>
      Some(sources.GreaterThanOrEqual(name, convertToScala(v, t)))

    case expressions.InSet(e @ pushableColumn(name), set) =>
      val toScala = CatalystTypeConverters.createToScalaConverter(e.dataType)
      Some(sources.In(name, set.toArray.map(toScala)))

    // Because we only convert In to InSet in Optimizer when there are more than certain
    // items. So it is possible we still get an In expression here that needs to be pushed
    // down.
    case expressions.In(e @ pushableColumn(name), list) if list.forall(_.isInstanceOf[Literal]) =>
      val hSet = list.map(_.eval(EmptyRow))
      val toScala = CatalystTypeConverters.createToScalaConverter(e.dataType)
      Some(sources.In(name, hSet.toArray.map(toScala)))

    case expressions.IsNull(pushableColumn(name)) =>
      Some(sources.IsNull(name))
    case expressions.IsNotNull(pushableColumn(name)) =>
      Some(sources.IsNotNull(name))
    case expressions.StartsWith(pushableColumn(name), Literal(v: UTF8String, StringType)) =>
      Some(sources.StringStartsWith(name, v.toString))

    case expressions.EndsWith(pushableColumn(name), Literal(v: UTF8String, StringType)) =>
      Some(sources.StringEndsWith(name, v.toString))

    case expressions.Contains(pushableColumn(name), Literal(v: UTF8String, StringType)) =>
      Some(sources.StringContains(name, v.toString))

    case expressions.Literal(true, BooleanType) =>
      Some(sources.AlwaysTrue)

    case expressions.Literal(false, BooleanType) =>
      Some(sources.AlwaysFalse)

    case _ => None
  }

  /**
   * Tries to translate a Catalyst [[Expression]] into data source [[Filter]].
   *
   * @return a `Some[Filter]` if the input [[Expression]] is convertible, otherwise a `None`.
   */
  protected[sql] def translateFilter(
      predicate: Expression, supportNestedPredicatePushdown: Boolean): Option[Filter] = {
    translateFilterWithMapping(predicate, None, supportNestedPredicatePushdown)
  }

  /**
   * Tries to translate a Catalyst [[Expression]] into data source [[Filter]].
   *
   * @param predicate The input [[Expression]] to be translated as [[Filter]]
   * @param translatedFilterToExpr An optional map from leaf node filter expressions to its
   *                               translated [[Filter]]. The map is used for rebuilding
   *                               [[Expression]] from [[Filter]].
   * @param nestedPredicatePushdownEnabled Whether nested predicate pushdown is enabled.
   * @return a `Some[Filter]` if the input [[Expression]] is convertible, otherwise a `None`.
   */
  protected[sql] def translateFilterWithMapping(
      predicate: Expression,
      translatedFilterToExpr: Option[mutable.HashMap[sources.Filter, Expression]],
      nestedPredicatePushdownEnabled: Boolean)
  : Option[Filter] = {
    predicate match {
      case expressions.And(left, right) =>
        // See SPARK-12218 for detailed discussion
        // It is not safe to just convert one side if we do not understand the
        // other side. Here is an example used to explain the reason.
        // Let's say we have (a = 2 AND trim(b) = 'blah') OR (c > 0)
        // and we do not understand how to convert trim(b) = 'blah'.
        // If we only convert a = 2, we will end up with
        // (a = 2) OR (c > 0), which will generate wrong results.
        // Pushing one leg of AND down is only safe to do at the top level.
        // You can see ParquetFilters' createFilter for more details.
        for {
          leftFilter <- translateFilterWithMapping(
            left, translatedFilterToExpr, nestedPredicatePushdownEnabled)
          rightFilter <- translateFilterWithMapping(
            right, translatedFilterToExpr, nestedPredicatePushdownEnabled)
        } yield sources.And(leftFilter, rightFilter)

      case expressions.Or(left, right) =>
        for {
          leftFilter <- translateFilterWithMapping(
            left, translatedFilterToExpr, nestedPredicatePushdownEnabled)
          rightFilter <- translateFilterWithMapping(
            right, translatedFilterToExpr, nestedPredicatePushdownEnabled)
        } yield sources.Or(leftFilter, rightFilter)

      case expressions.Not(child) =>
        translateFilterWithMapping(child, translatedFilterToExpr, nestedPredicatePushdownEnabled)
          .map(sources.Not)

      case other =>
        val filter = translateLeafNodeFilter(other, PushableColumn(nestedPredicatePushdownEnabled))
        if (filter.isDefined && translatedFilterToExpr.isDefined) {
          translatedFilterToExpr.get(filter.get) = predicate
        }
        filter
    }
  }

  protected[sql] def rebuildExpressionFromFilter(
      filter: Filter,
      translatedFilterToExpr: mutable.HashMap[sources.Filter, Expression]): Expression = {
    filter match {
      case sources.And(left, right) =>
        expressions.And(rebuildExpressionFromFilter(left, translatedFilterToExpr),
          rebuildExpressionFromFilter(right, translatedFilterToExpr))
      case sources.Or(left, right) =>
        expressions.Or(rebuildExpressionFromFilter(left, translatedFilterToExpr),
          rebuildExpressionFromFilter(right, translatedFilterToExpr))
      case sources.Not(pred) =>
        expressions.Not(rebuildExpressionFromFilter(pred, translatedFilterToExpr))
      case other =>
        translatedFilterToExpr.getOrElse(other,
          throw new AnalysisException(
            s"Fail to rebuild expression: missing key $filter in `translatedFilterToExpr`"))
    }
  }

  /**
   * Selects Catalyst predicate [[Expression]]s which are convertible into data source [[Filter]]s
   * and can be handled by `relation`.
   *
   * @return A triplet of `Seq[Expression]`, `Seq[Filter]`, and `Seq[Filter]` . The first element
   *         contains all Catalyst predicate [[Expression]]s that are either not convertible or
   *         cannot be handled by `relation`. The second element contains all converted data source
   *         [[Filter]]s that will be pushed down to the data source. The third element contains
   *         all [[Filter]]s that are completely filtered at the DataSource.
   */
  protected[sql] def selectFilters(
      relation: BaseRelation,
      predicates: Seq[Expression]): (Seq[Expression], Seq[Filter], Set[Filter]) = {

    // For conciseness, all Catalyst filter expressions of type `expressions.Expression` below are
    // called `predicate`s, while all data source filters of type `sources.Filter` are simply called
    // `filter`s.

    // A map from original Catalyst expressions to corresponding translated data source filters.
    // If a predicate is not in this map, it means it cannot be pushed down.
    val supportNestedPredicatePushdown = DataSourceUtils.supportNestedPredicatePushdown(relation)
    val translatedMap: Map[Expression, Filter] = predicates.flatMap { p =>
      translateFilter(p, supportNestedPredicatePushdown).map(f => p -> f)
    }.toMap

    val pushedFilters: Seq[Filter] = translatedMap.values.toSeq

    // Catalyst predicate expressions that cannot be converted to data source filters.
    val nonconvertiblePredicates = predicates.filterNot(translatedMap.contains)

    // Data source filters that cannot be handled by `relation`. An unhandled filter means
    // the data source cannot guarantee the rows returned can pass the filter.
    // As a result we must return it so Spark can plan an extra filter operator.
    val unhandledFilters = relation.unhandledFilters(translatedMap.values.toArray).toSet
    val unhandledPredicates = translatedMap.filter { case (p, f) =>
      unhandledFilters.contains(f)
    }.keys
    val handledFilters = pushedFilters.toSet -- unhandledFilters

    (nonconvertiblePredicates ++ unhandledPredicates, pushedFilters, handledFilters)
  }

  /**
   * Convert RDD of Row into RDD of InternalRow with objects in catalyst types
   */
  private[sql] def toCatalystRDD(
      relation: BaseRelation,
      output: Seq[Attribute],
      rdd: RDD[Row]): RDD[InternalRow] = {
    if (relation.needConversion) {
      val toRow = RowEncoder(StructType.fromAttributes(output)).createSerializer()
      rdd.mapPartitions { iterator =>
        iterator.map(toRow)
      }
    } else {
      rdd.asInstanceOf[RDD[InternalRow]]
    }
  }
}

/**
 * Find the column name of an expression that can be pushed down.
 */
abstract class PushableColumnBase {
  val nestedPredicatePushdownEnabled: Boolean

  def unapply(e: Expression): Option[String] = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
    def helper(e: Expression): Option[Seq[String]] = e match {
      case a: Attribute =>
        if (nestedPredicatePushdownEnabled || !a.name.contains(".")) {
          Some(Seq(a.name))
        } else {
          None
        }
      case s: GetStructField if nestedPredicatePushdownEnabled =>
        helper(s.child).map(_ :+ s.childSchema(s.ordinal).name)
      case _ => None
    }
    helper(e).map(_.quoted)
  }
}

object PushableColumn {
  def apply(nestedPredicatePushdownEnabled: Boolean): PushableColumnBase = {
    if (nestedPredicatePushdownEnabled) {
      PushableColumnAndNestedColumn
    } else {
      PushableColumnWithoutNestedColumn
    }
  }
}

object PushableColumnAndNestedColumn extends PushableColumnBase {
  override val nestedPredicatePushdownEnabled = true
}

object PushableColumnWithoutNestedColumn extends PushableColumnBase {
  override val nestedPredicatePushdownEnabled = false
}
