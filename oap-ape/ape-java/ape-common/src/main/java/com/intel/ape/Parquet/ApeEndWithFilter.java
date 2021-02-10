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

package com.intel.ape.Parquet;

import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveComparator;


public class ApeEndWithFilter extends ApeLikeFilter {
  public ApeEndWithFilter(String value) {
    super(value);
  }

  @Override
  public boolean keep(Binary binary) {
    return binary != null;
  }

  @Override
  public boolean canDrop(Statistics<Binary> statistics) {
    PrimitiveComparator<Binary> comparator = PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR;
    Binary max = statistics.getMax();
    Binary min = statistics.getMin();
    int lenInMax = Math.min(size, max.length());
    int lenInMin = Math.min(size, min.length());
    return comparator.compare(max.slice(max.length() - lenInMax, lenInMax), strToBinary) < 0 ||
            comparator.compare(min.slice(min.length() - lenInMin, lenInMin), strToBinary) > 0;
  }

  @Override
  public boolean inverseCanDrop(Statistics<Binary> statistics) {
    PrimitiveComparator<Binary> comparator = PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR;
    Binary max = statistics.getMax();
    Binary min = statistics.getMin();
    int lenInMax = Math.min(size, max.length());
    int lenInMin = Math.min(size, min.length());
    return comparator.compare(max.slice(max.length() - lenInMax, lenInMax), strToBinary) == 0 &&
            comparator.compare(min.slice(min.length() - lenInMin, lenInMin), strToBinary) == 0;
  }

}
