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

package com.intel.ape.util;

import org.apache.parquet.filter2.predicate.*;

import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class ParquetFilterPredicateConvertorTest {
  private static final Operators.IntColumn intColumn = intColumn("a.b.c");
  private static final Operators.LongColumn longColumn = longColumn("a.b.l");
  private static final Operators.DoubleColumn doubleColumn = doubleColumn("x.y.z");
  private static final Operators.BinaryColumn binColumn = binaryColumn("a.string.column");

  public static void main(String[] args) {
    test1();
  }

  static void test1() {
    FilterPredicate predicate =
            and(not(or(eq(intColumn, 7), notEq(intColumn, 17))), gt(doubleColumn, 100.0));
    String out = ParquetFilterPredicateConvertor.toJsonString(predicate);
    System.out.println(out);
  }


}
