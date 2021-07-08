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
import java.util.Arrays;

/**
 * Aggregate expression super class.
 */
public class AggregateExpr implements Serializable {
    protected String exprName;
    protected String dataType;

    // optional attributes
    protected String aliasName;
    protected String castType;
    protected Boolean promotePrecision;
    protected Boolean checkOverflow;
    protected String checkOverflowType;
    protected Boolean nullOnOverFlow;

    public AggregateExpr(String exprName, String dataType) {
        this.exprName = exprName;
        this.dataType = dataType;
    }

    public String getExprName() {
        return exprName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public void setExprName(String exprName) {
        this.exprName = exprName;
    }

    public String getAliasName() {
        return aliasName;
    }

    public void setAliasName(String aliasName) {
        this.aliasName = aliasName;
    }

    public String getCastType() {
        return castType;
    }

    public void setCastType(String castType) {
        this.castType = castType;
    }

    public Boolean getPromotePrecision() {
        return promotePrecision;
    }

    public void setPromotePrecision(Boolean promotePrecision) {
        this.promotePrecision = promotePrecision;
    }

    public Boolean getCheckOverflow() {
        return checkOverflow;
    }

    public void setCheckOverflow(Boolean checkOverflow) {
        this.checkOverflow = checkOverflow;
    }

    public String getCheckOverflowType() {
        return checkOverflowType;
    }

    public void setCheckOverflowType(String checkOverflowType) {
        this.checkOverflowType = checkOverflowType;
    }

    public Boolean getNullOnOverFlow() {
        return nullOnOverFlow;
    }

    public void setNullOnOverFlow(Boolean nullOnOverFlow) {
        this.nullOnOverFlow = nullOnOverFlow;
    }

    public void translateDataTypeName() {
        dataType = translateDataTypeNameInternal(dataType);
        checkOverflowType = translateDataTypeNameInternal(checkOverflowType);
    }

    private String translateDataTypeNameInternal(String dataType) {
        if (dataType != null) {
            if (Arrays.asList("BooleanType", "IntegerType", "LongType", "FloatType", "DoubleType",
                    "StringType").contains(dataType)
                || dataType.startsWith("DecimalType")) {
                // no need to translate
                return dataType;
            }

            if (Arrays.asList("BOOLEAN", "INTEGER", "FLOAT", "DOUBLE")
                .contains(dataType)) {
                dataType = dataType.substring(0, 1).toUpperCase()
                    + dataType.substring(1).toLowerCase()
                    + "Type";
            } else if ("BIGINT".equals(dataType)) {
                dataType = "LongType";
            } else if (dataType.startsWith("VARCHAR")) {
                dataType = "StringType";
            } else if (dataType.startsWith("DECIMAL")) {
                dataType = dataType.replace("DECIMAL", "DecimalType");
            } else {
                throw new RuntimeException("Unsupported data type name in AggregateExpr");
            }
        }

        return dataType;
    }

    public boolean checkDataTypes() {
        return true;
    }
}
