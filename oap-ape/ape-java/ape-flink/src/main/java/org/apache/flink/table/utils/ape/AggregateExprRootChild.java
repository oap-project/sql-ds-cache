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

package org.apache.flink.table.utils.ape;

/**
 * Aggregate expression root child.
 */
public class AggregateExprRootChild extends AggregateExpr {

	private AggregateExpr child;

	public AggregateExprRootChild(Type type, AggregateExpr c) {
		super(type.name, null);
		child = c;
	}

	public AggregateExpr getChild() {
		return child;
	}

	public void setChild(AggregateExpr child) {
		this.child = child;
	}

	/**
	 * Enum of supported aggregate operations.
	 */
	public enum Type {
		MAX("Max"),
		MIN("Min"),
		COUNT("Count"),
		SUM("Sum"); /* `Average` will be parse to `Sum` and `Count`. So there is no `Average` */

		public String name;
		Type(String n) {
			name = n;
		}

		public static Type fromSqlKind(String sql) {
			switch (sql) {
				case "max":
					return Type.MAX;
				case "min":
					return Type.MIN;
				case "count":
				case "count1":
					return Type.COUNT;
				case "sum":
					return Type.SUM;
				default:
					return null;
			}
		}
	}

	@Override
	public void translateDataTypeName() {
		super.translateDataTypeName();

		child.translateDataTypeName();
	}

	public boolean checkDataTypes() {
		return child.checkDataTypes();
	}
}
