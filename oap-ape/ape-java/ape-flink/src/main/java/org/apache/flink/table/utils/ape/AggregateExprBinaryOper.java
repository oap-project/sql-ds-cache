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
 * Aggregate expression binary operator.
 */
public class AggregateExprBinaryOper extends AggregateExpr {

    private AggregateExpr leftNode;
    private AggregateExpr rightNode;

    public AggregateExprBinaryOper(
        Type type,
        String dataType,
        AggregateExpr leftNode,
        AggregateExpr rightNode) {
        super(type.name, dataType);
        this.dataType = dataType;
        this.leftNode = leftNode;
        this.rightNode = rightNode;
    }

    public AggregateExpr getLeftNode() {
        return leftNode;
    }

    public void setLeftNode(AggregateExpr leftNode) {
        this.leftNode = leftNode;
    }

    public AggregateExpr getRightNode() {
        return rightNode;
    }

    public void setRightNode(AggregateExpr rightNode) {
        this.rightNode = rightNode;
    }

    /**
     * Enum for supported binary operations.
     */
    public enum Type {
        ADD("Add"),
        SUBTRACT("Subtract"),
        MULTIPLY("Multiply"),
        DIVIDE("Divide"),
        MOD("Mod");

        public String name;
        Type(String n) {
            name = n;
        }

        public static Type fromSqlKind(String sql) {
            switch (sql) {
                case "PLUS":
                    return Type.ADD;
                case "MINUS":
                    return Type.SUBTRACT;
                case "TIMES":
                    return Type.MULTIPLY;
                case "DIVIDE":
                    return Type.DIVIDE;
                case "MOD":
                    return Type.MOD;
                default:
                    return null;
            }
        }
    }

    @Override
    public void translateDataTypeName() {
        super.translateDataTypeName();

        leftNode.translateDataTypeName();
        rightNode.translateDataTypeName();
    }

    public boolean checkDataTypes() {
        return leftNode.checkDataTypes() && rightNode.checkDataTypes();
    }
}
