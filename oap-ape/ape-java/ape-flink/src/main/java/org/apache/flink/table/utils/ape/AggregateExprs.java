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

import java.io.Serializable;
import java.util.List;

/**
 * Class to hold the whole aggregate expressions to be pushed down.
 */
public class AggregateExprs implements Serializable {
    private List<AggregateExpr> groupByExprs;
    private List<AggregateExpr> aggregateExprs;

    public AggregateExprs(List<AggregateExpr> groupByExprs, List<AggregateExpr> aggregateExprs) {
        this.groupByExprs = groupByExprs;
        this.aggregateExprs = aggregateExprs;
    }

    public List<AggregateExpr> getGroupByExprs() {
        return groupByExprs;
    }

    public void setGroupByExprs(List<AggregateExpr> groupByExprs) {
        this.groupByExprs = groupByExprs;
    }

    public List<AggregateExpr> getAggregateExprs() {
        return aggregateExprs;
    }

    public void setAggregateExprs(List<AggregateExpr> aggregateExprs) {
        this.aggregateExprs = aggregateExprs;
    }

    public void translateDataTypeName() {
        aggregateExprs.forEach(AggregateExpr::translateDataTypeName);
    }

    public boolean checkDataTypes() {
        return aggregateExprs.stream().allMatch(AggregateExpr::checkDataTypes);
    }
}
