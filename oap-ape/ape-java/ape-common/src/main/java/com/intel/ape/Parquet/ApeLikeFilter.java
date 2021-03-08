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
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveComparator;

import java.io.Serializable;

public abstract class ApeLikeFilter extends UserDefinedPredicate<Binary> implements Serializable {
  public ApeLikeFilter(String value) {
    this.value = value;
    this.strToBinary = Binary.fromReusedByteArray(value.getBytes());
    this.size = strToBinary.length();
  }

  protected String value;
  protected Binary strToBinary;
  protected int size;

  public String getValue() {
    return value;
  }

}
