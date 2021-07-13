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
 * Aggregate expression root.
 */
public class AggregateExprRoot extends AggregateExpr {
    public static final String DEFAULT_NAME = "RootAgg";

    private boolean isDistinct;

    private AggregateExprRootChild child;

    public AggregateExprRoot(
        boolean isDistinct,
        AggregateExprRootChild child) {
        super(DEFAULT_NAME, null);
        this.isDistinct = isDistinct;
        this.child = child;
    }

    public boolean getIsDistinct() {
        return isDistinct;
    }

    public void setIsDistinct(boolean distinct) {
        isDistinct = distinct;
    }

    public AggregateExpr getChild() {
        return child;
    }

    public void setChild(AggregateExprRootChild child) {
        this.child = child;
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
