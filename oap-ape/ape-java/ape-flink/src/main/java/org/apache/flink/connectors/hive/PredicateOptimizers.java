/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.hive;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;

import javax.annotation.Nullable;

/**
 * Optimizers to improve predicate tree.
 */
public class PredicateOptimizers {

	/**
	 * Add an `and` `is not null` brother to existing column predicate.
	 */
	static class AddNotNull implements FilterPredicate.Visitor<FilterPredicate> {
		private FilterPredicate parent = null;

		@Override
		public <T extends Comparable<T>> FilterPredicate visit(Operators.Eq<T> eq) {
			// `is null` does not need to add `not null` predicate
			if (eq.getValue() == null) {
				return eq;
			}

			if (needToBeNotNull(eq, eq.getColumn().getColumnPath())) {
				return andNotNull(eq, eq.getColumn());
			}

			return eq;
		}

		@Override
		public <T extends Comparable<T>> FilterPredicate visit(Operators.NotEq<T> notEq) {
			// `is not null` does not need to add `not null` predicate
			if (notEq.getValue() == null) {
				return notEq;
			}

			if (needToBeNotNull(notEq, notEq.getColumn().getColumnPath())) {
				return andNotNull(notEq, notEq.getColumn());
			}

			return notEq;
		}

		@Override
		public <T extends Comparable<T>> FilterPredicate visit(Operators.Lt<T> lt) {
			if (needToBeNotNull(lt, lt.getColumn().getColumnPath())) {
				return andNotNull(lt, lt.getColumn());
			}

			return lt;
		}

		@Override
		public <T extends Comparable<T>> FilterPredicate visit(Operators.LtEq<T> ltEq) {
			if (needToBeNotNull(ltEq, ltEq.getColumn().getColumnPath())) {
				return andNotNull(ltEq, ltEq.getColumn());
			}

			return ltEq;
		}

		@Override
		public <T extends Comparable<T>> FilterPredicate visit(Operators.Gt<T> gt) {
			if (needToBeNotNull(gt, gt.getColumn().getColumnPath())) {
				return andNotNull(gt, gt.getColumn());
			}

			return gt;
		}

		@Override
		public <T extends Comparable<T>> FilterPredicate visit(Operators.GtEq<T> gtEq) {
			if (needToBeNotNull(gtEq, gtEq.getColumn().getColumnPath())) {
				return andNotNull(gtEq, gtEq.getColumn());
			}

			return gtEq;
		}

		// Check whether we need to add a `not null` predicate to a existing column predicate.
		// Return false only if there exists an `and` brother which is `is not null` already.
		boolean needToBeNotNull(FilterPredicate predicate, ColumnPath columnPath) {
			if (!(parent instanceof Operators.And)) {
				return true;
			}

			boolean ret = true;

			Operators.And andParent = (Operators.And) parent;
			FilterPredicate brother =
				andParent.getLeft() == predicate ? andParent.getRight() : andParent.getLeft();
			if (brother instanceof Operators.NotEq) {
				Operators.NotEq notEq = (Operators.NotEq) brother;

				if (notEq.getColumn().getColumnPath().equals(columnPath)
					&& notEq.getValue() == null) {
					// already has an `and` brother which means `is not null` for the same column
					ret = false;
				}
			}

			return ret;
		}

		@Nullable
		private FilterPredicate notEquals(Tuple2<Operators.Column, Comparable> columnPair) {
			if (columnPair.f0 instanceof Operators.IntColumn) {
				return FilterApi.notEq(
					(Operators.IntColumn) columnPair.f0,
					(Integer) columnPair.f1);
			} else if (columnPair.f0 instanceof Operators.LongColumn) {
				return FilterApi.notEq((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
			} else if (columnPair.f0 instanceof Operators.DoubleColumn) {
				return FilterApi.notEq(
					(Operators.DoubleColumn) columnPair.f0,
					(Double) columnPair.f1);
			} else if (columnPair.f0 instanceof Operators.FloatColumn) {
				return FilterApi.notEq(
					(Operators.FloatColumn) columnPair.f0,
					(Float) columnPair.f1);
			} else if (columnPair.f0 instanceof Operators.BooleanColumn) {
				return FilterApi.notEq(
					(Operators.BooleanColumn) columnPair.f0,
					(Boolean) columnPair.f1);
			} else if (columnPair.f0 instanceof Operators.BinaryColumn) {
				return FilterApi.notEq(
					(Operators.BinaryColumn) columnPair.f0,
					(Binary) columnPair.f1);
			}

			return null;
		}

		private FilterPredicate andNotNull(FilterPredicate predicate, Operators.Column column) {
			FilterPredicate notEqNull = notEquals(new Tuple2<>(column, null));
			if (notEqNull != null) {
				return FilterApi.and(notEqNull, predicate);
			}

			return predicate;
		}

		@Override
		public FilterPredicate visit(Operators.And and) {
			parent = and;
			FilterPredicate left = and.getLeft().accept(this);

			parent = and;
			FilterPredicate right = and.getRight().accept(this);

			return FilterApi.and(left, right);
		}

		@Override
		public FilterPredicate visit(Operators.Or or) {
			parent = or;
			FilterPredicate left = or.getLeft().accept(this);

			parent = or;
			FilterPredicate right = or.getRight().accept(this);

			return FilterApi.or(left, right);
		}

		@Override
		public FilterPredicate visit(Operators.Not not) {
			parent = not;

			FilterPredicate predicate = not.getPredicate().accept(this);

			return FilterApi.not(predicate);
		}

		@Override
		public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> FilterPredicate visit(
			Operators.UserDefined<T, U> userDefined) {
			return userDefined;
		}

		@Override
		public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> FilterPredicate visit(
			Operators.LogicalNotUserDefined<T, U> logicalNotUserDefined) {
			return logicalNotUserDefined;
		}
	}
}
