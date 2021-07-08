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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.intel.ape.parquet.ApeContainsFilter;
import com.intel.ape.parquet.ApeEndWithFilter;
import com.intel.ape.parquet.ApeStartWithFilter;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;


/**
 * Visit expression to generator {@link FilterPredicate}.
 */
public class ExpressionToPredicateConverter implements ExpressionVisitor<FilterPredicate> {
    private static final String FUNC_NOT = "not";
    private static final String FUNC_AND = "and";
    private static final String FUNC_OR = "or";
    private static final String FUNC_IS_NULL = "isNull";
    private static final String FUNC_IS_NOT_NULL = "isNotNull";
    private static final String FUNC_EQUALS = "equals";
    private static final String FUNC_NOT_EQUALS = "notEquals";
    private static final String FUNC_GREATER_THAN = "greaterThan";
    private static final String FUNC_GREATER_THAN_OR_EQUAL = "greaterThanOrEqual";
    private static final String FUNC_LESS_THAN = "lessThan";
    private static final String FUNC_LESS_THAN_OR_EQUAL = "lessThanOrEqual";
    private static final String FUNC_LIKE = "like";

    private static final int MILLIS_PER_DAY = 86400 * 1000;

    @Override
    public FilterPredicate visit(CallExpression call) {
        if (!(call.getFunctionDefinition() instanceof BuiltInFunctionDefinition)) {
            return null;
        }

        String name = ((BuiltInFunctionDefinition) call.getFunctionDefinition()).getName();
        switch (name) {
            case FUNC_NOT:
                FilterPredicate child = call.getChildren().get(0).accept(this);
                if (child != null) {
                    return FilterApi.not(child);
                }
                break;
            case FUNC_AND:
                FilterPredicate ac1 = call.getChildren().get(0).accept(this);
                FilterPredicate ac2 = call.getChildren().get(1).accept(this);
                if (ac1 != null && ac2 != null) {
                    return FilterApi.and(ac1, ac2);
                }
                break;
            case FUNC_OR:
                FilterPredicate oc1 = call.getChildren().get(0).accept(this);
                FilterPredicate oc2 = call.getChildren().get(1).accept(this);
                if (oc1 != null && oc2 != null) {
                    return FilterApi.or(oc1, oc2);
                }
                break;
            case FUNC_IS_NULL:
            case FUNC_IS_NOT_NULL:
                Operators.Column column = extractColumn(call.getResolvedChildren());

                if (column == null) {
                    return null;
                }

                switch (name) {
                    case FUNC_IS_NULL:
                        return equals(new Tuple2<>(column, null));
                    case FUNC_IS_NOT_NULL:
                        return notEquals(new Tuple2<>(column, null));
                    default:
                        return null;
                }

            case FUNC_EQUALS:
            case FUNC_NOT_EQUALS:
            case FUNC_GREATER_THAN:
            case FUNC_GREATER_THAN_OR_EQUAL:
            case FUNC_LESS_THAN:
            case FUNC_LESS_THAN_OR_EQUAL:
            case FUNC_LIKE:
                Tuple2<Operators.Column, Comparable> columnPair =
                    extractColumnAndLiteral(call.getResolvedChildren());

                if (columnPair == null) {
                    return null;
                }

                boolean onRight = literalOnRight(call.getResolvedChildren());
                switch (name) {
                    case FUNC_EQUALS:
                        return equals(columnPair);
                    case FUNC_NOT_EQUALS:
                        return notEquals(columnPair);
                    case FUNC_GREATER_THAN:
                        if (onRight) {
                            return greaterThan(columnPair);
                        } else {
                            lessThan(columnPair);
                        }
                        break;
                    case FUNC_GREATER_THAN_OR_EQUAL:
                        if (onRight) {
                            return greaterThanOrEqual(columnPair);
                        } else {
                            return lessThanOrEqual(columnPair);
                        }
                    case FUNC_LESS_THAN:
                        if (onRight) {
                            return lessThan(columnPair);
                        } else {
                            return greaterThan(columnPair);
                        }
                    case FUNC_LESS_THAN_OR_EQUAL:
                        if (onRight) {
                            return lessThanOrEqual(columnPair);
                        } else {
                            return greaterThanOrEqual(columnPair);
                        }
                    case FUNC_LIKE:
                        return like(columnPair, call.getResolvedChildren());
                    default:
                        // Unsupported Predicate
                        return null;
                }
        }

        return null;
    }

    @Nullable
    private FilterPredicate equals(Tuple2<Operators.Column, Comparable> columnPair) {
        if (columnPair.f0 instanceof Operators.IntColumn) {
            return FilterApi.eq((Operators.IntColumn) columnPair.f0, (Integer) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.LongColumn) {
            return FilterApi.eq((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.DoubleColumn) {
            return FilterApi.eq((Operators.DoubleColumn) columnPair.f0, (Double) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.FloatColumn) {
            return FilterApi.eq((Operators.FloatColumn) columnPair.f0, (Float) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.BooleanColumn) {
            return FilterApi.eq((Operators.BooleanColumn) columnPair.f0, (Boolean) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.BinaryColumn) {
            return FilterApi.eq((Operators.BinaryColumn) columnPair.f0, (Binary) columnPair.f1);
        }

        return null;
    }

    @Nullable
    private FilterPredicate notEquals(Tuple2<Operators.Column, Comparable> columnPair) {
        if (columnPair.f0 instanceof Operators.IntColumn) {
            return FilterApi.notEq((Operators.IntColumn) columnPair.f0, (Integer) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.LongColumn) {
            return FilterApi.notEq((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.DoubleColumn) {
            return FilterApi.notEq((Operators.DoubleColumn) columnPair.f0, (Double) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.FloatColumn) {
            return FilterApi.notEq((Operators.FloatColumn) columnPair.f0, (Float) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.BooleanColumn) {
            return FilterApi.notEq(
                (Operators.BooleanColumn) columnPair.f0,
                (Boolean) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.BinaryColumn) {
            return FilterApi.notEq((Operators.BinaryColumn) columnPair.f0, (Binary) columnPair.f1);
        }

        return null;
    }

    @Nullable
    private FilterPredicate greaterThan(Tuple2<Operators.Column, Comparable> columnPair) {
        if (columnPair.f0 instanceof Operators.IntColumn) {
            return FilterApi.gt((Operators.IntColumn) columnPair.f0, (Integer) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.LongColumn) {
            return FilterApi.gt((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.DoubleColumn) {
            return FilterApi.gt((Operators.DoubleColumn) columnPair.f0, (Double) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.FloatColumn) {
            return FilterApi.gt((Operators.FloatColumn) columnPair.f0, (Float) columnPair.f1);
        }

        return null;
    }

    @Nullable
    private FilterPredicate lessThan(Tuple2<Operators.Column, Comparable> columnPair) {
        if (columnPair.f0 instanceof Operators.IntColumn) {
            return FilterApi.lt((Operators.IntColumn) columnPair.f0, (Integer) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.LongColumn) {
            return FilterApi.lt((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.DoubleColumn) {
            return FilterApi.lt((Operators.DoubleColumn) columnPair.f0, (Double) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.FloatColumn) {
            return FilterApi.lt((Operators.FloatColumn) columnPair.f0, (Float) columnPair.f1);
        }

        return null;
    }

    @Nullable
    private FilterPredicate greaterThanOrEqual(Tuple2<Operators.Column, Comparable> columnPair) {
        if (columnPair.f0 instanceof Operators.IntColumn) {
            return FilterApi.gtEq((Operators.IntColumn) columnPair.f0, (Integer) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.LongColumn) {
            return FilterApi.gtEq((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.DoubleColumn) {
            return FilterApi.gtEq((Operators.DoubleColumn) columnPair.f0, (Double) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.FloatColumn) {
            return FilterApi.gtEq((Operators.FloatColumn) columnPair.f0, (Float) columnPair.f1);
        }

        return null;
    }

    @Nullable
    private FilterPredicate lessThanOrEqual(Tuple2<Operators.Column, Comparable> columnPair) {
        if (columnPair.f0 instanceof Operators.IntColumn) {
            return FilterApi.ltEq((Operators.IntColumn) columnPair.f0, (Integer) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.LongColumn) {
            return FilterApi.ltEq((Operators.LongColumn) columnPair.f0, (Long) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.DoubleColumn) {
            return FilterApi.ltEq((Operators.DoubleColumn) columnPair.f0, (Double) columnPair.f1);
        } else if (columnPair.f0 instanceof Operators.FloatColumn) {
            return FilterApi.ltEq((Operators.FloatColumn) columnPair.f0, (Float) columnPair.f1);
        }

        return null;
    }

    private Operators.Column extractColumn(List<ResolvedExpression> children) {

        if (children == null || children.size() != 1) {
            return null;
        }

        boolean isValid = children.get(0) instanceof FieldReferenceExpression;

        if (!isValid) {
            return null;
        }

        String columnName = ((FieldReferenceExpression) children.get(0)).getName();
        DataType columnType = children.get(0).getOutputDataType();

        return makeColumn(columnName, columnType);

    }

    private Tuple2<Operators.Column, Comparable> extractColumnAndLiteral(
        List<ResolvedExpression> children) {

        // check column type and value type match
        boolean isValid = checkColumnAndLiteralTypes(children);

        if (!isValid) {
            return null;
        }

        Operators.Column column = getColumn(children);
        Comparable value = getValue(children);

        if (column != null
            && value != null
            && isDecimalColumn(children)) {

            int columnScale = getDecimalColumnScale(children);
            value = getDecimalValueMatchingColumnScale(column, columnScale, value);
        }

        if (column == null || value == null) {
            return null;
        }

        return new Tuple2<>(column, value);

    }

    /**
     * set value scale to match column type's scale when it's Decimal.
     * @param column
     * @param columnScale
     * @param literal
     * @return
     */
    private Comparable getDecimalValueMatchingColumnScale(
        Operators.Column column, int columnScale, Comparable literal) {

        BigDecimal literalDecimal = new BigDecimal(literal.toString());
        if (literalDecimal.scale() > columnScale) {
            // not supported
            return null;
        } else if (literalDecimal.scale() < columnScale) {
            // promote the scale of literal value
            literalDecimal = literalDecimal.setScale(columnScale, BigDecimal.ROUND_UNNECESSARY);
        }

        Comparable value = null;
        BigInteger literalInteger = literalDecimal.unscaledValue();

        // the literal value's type shouldn't be larger than the type of column
        if (column instanceof Operators.IntColumn
            && DecimalDataUtils.is32BitDecimal(literalDecimal.precision())) {
            value = literalInteger.intValue();
        } else if (
            column instanceof Operators.LongColumn
            && (DecimalDataUtils.is32BitDecimal(literalDecimal.precision())
                || DecimalDataUtils.is64BitDecimal(literalDecimal.precision())
            )
        ) {
            value = literalInteger.longValue();
        }

        return value;
    }

    private boolean isDecimalColumn(List<ResolvedExpression> children) {
        int columnIndex = 0;
        if (!literalOnRight(children)) {
            columnIndex = 1;
        }

        DataType columnType = children.get(columnIndex).getOutputDataType();

        return columnType.getLogicalType().getTypeRoot() == LogicalTypeRoot.DECIMAL;
    }

    private int getDecimalColumnScale(List<ResolvedExpression> children) {
        int columnIndex = 0;
        if (!literalOnRight(children)) {
            columnIndex = 1;
        }

        DataType columnType = children.get(columnIndex).getOutputDataType();

        if (columnType.getLogicalType() instanceof DecimalType) {
            return ((DecimalType) columnType.getLogicalType()).getScale();
        } else {
            throw new RuntimeException("Invalid call to getDecimalColumnScale");
        }
    }

    private boolean checkColumnAndLiteralTypes(List<ResolvedExpression> children) {
        if (children == null || children.size() != 2) {
            return false;
        }

        boolean isValid = (
            (children.get(0) instanceof FieldReferenceExpression
                && children.get(1) instanceof ValueLiteralExpression)
                ||
                (children.get(1) instanceof FieldReferenceExpression
                    && children.get(0) instanceof ValueLiteralExpression));

        if (!isValid) {
            return false;
        }

        int columnIndex = 0;
        int valueIndex = 1;
        if (!literalOnRight(children)) {
            columnIndex = 1;
            valueIndex = 0;
        }

        LogicalTypeRoot columnType =
            children.get(columnIndex).getOutputDataType().getLogicalType().getTypeRoot();
        LogicalTypeRoot valueType =
            children.get(valueIndex).getOutputDataType().getLogicalType().getTypeRoot();

        // column type and value type should match
        switch (columnType) {
            case BOOLEAN:
                return valueType == LogicalTypeRoot.BOOLEAN;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                // integer numbers are all INTEGER in ValueLiteralExpressions
                return valueType == LogicalTypeRoot.INTEGER;
            case DATE:
                return valueType == LogicalTypeRoot.DATE;
            case BIGINT:
                return valueType == LogicalTypeRoot.BIGINT || valueType == LogicalTypeRoot.INTEGER;
            case FLOAT:
                return valueType == LogicalTypeRoot.FLOAT || valueType == LogicalTypeRoot.INTEGER;
            case DOUBLE:
                return valueType == LogicalTypeRoot.DOUBLE
                    || valueType == LogicalTypeRoot.FLOAT
                    || valueType == LogicalTypeRoot.BIGINT
                    || valueType == LogicalTypeRoot.INTEGER;
            case DECIMAL:
            case BINARY:
            case VARBINARY:
            case CHAR:
            case VARCHAR:
                // No data loss between values and strings in these types
                return true;
            default:
                // unsupported types
                return false;
        }

    }

    private boolean literalOnRight(List<ResolvedExpression> children) {
        return children.get(1) instanceof ValueLiteralExpression;
    }

    @Nullable
    private Operators.Column getColumn(List<ResolvedExpression> children) {
        int columnIndex = 0;
        if (!literalOnRight(children)) {
            columnIndex = 1;
        }

        String columnName = ((FieldReferenceExpression) children.get(columnIndex)).getName();
        DataType columnType = children.get(columnIndex).getOutputDataType();

        return makeColumn(columnName, columnType);

    }

    @Nullable
    private Operators.Column makeColumn(String columnName, DataType columnType) {

        switch (columnType.getLogicalType().getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case DATE:
            case INTEGER:
                return FilterApi.intColumn(columnName);
            case BIGINT:
                return FilterApi.longColumn(columnName);
            case FLOAT:
                return FilterApi.floatColumn(columnName);
            case DOUBLE:
                return FilterApi.doubleColumn(columnName);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) columnType.getLogicalType();
                if (DecimalDataUtils.is32BitDecimal(decimalType.getPrecision())) {
                    return FilterApi.intColumn(columnName);
                } else if (DecimalDataUtils.is64BitDecimal(decimalType.getPrecision())) {
                    return FilterApi.longColumn(columnName);
                } else {
                    return null;
                }
            case BINARY:
            case VARBINARY:
            case CHAR:
            case VARCHAR:
                return FilterApi.binaryColumn(columnName);
            case BOOLEAN:
                return FilterApi.booleanColumn(columnName);
            default:
                // unsupported types
                return null;
        }

    }

    @Nullable
    private Comparable getValue(List<ResolvedExpression> children) {
        int valueIndex = 1;
        if (!literalOnRight(children)) {
            valueIndex = 0;
        }

        ValueLiteralExpression ex = (ValueLiteralExpression) children.get(valueIndex);
        DataType valueType = ex.getOutputDataType();

        switch (valueType.getLogicalType().getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return ex.getValueAs(Integer.class).orElse(null);
            case DATE:
                Object obj = ex.getValueAs(Object.class).orElse(null);
                return dateToDays(obj);
            case BIGINT:
                return ex.getValueAs(Long.class).orElse(null);
            case FLOAT:
                return ex.getValueAs(Float.class).orElse(null);
            case DOUBLE:
                return ex.getValueAs(Double.class).orElse(null);
            case DECIMAL:
                return ex.getValueAs(BigDecimal.class).orElse(null);
            case BINARY:
            case VARBINARY:
                byte[] bytes = ex.getValueAs(byte[].class).orElse(null);
                return bytes == null ? null : Binary.fromConstantByteArray(bytes);
            case CHAR:
            case VARCHAR:
                String s = ex.getValueAs(String.class).orElse(null);
                return s == null ? null : Binary.fromString(s);
            case BOOLEAN:
                return ex.getValueAs(Boolean.class).orElse(null);
            default:
                // unsupported types
                return null;
        }
    }

    private Integer dateToDays(Object obj) {
        if (obj instanceof Date) {
            Date date = (Date) obj;
            long millisUtc = date.getTime();
            long millisLocal = millisUtc + TimeZone.getDefault().getOffset(millisUtc);
            int julianDays = Math.toIntExact(Math.floorDiv(millisLocal, MILLIS_PER_DAY));

            Calendar utcCal = new Calendar.Builder()
                // `gregory` is a hybrid calendar that supports both
                // the Julian and Gregorian calendar systems
                .setCalendarType("gregory")
                .setTimeZone(TimeZone.getTimeZone("UTC"))
                .setInstant(Math.multiplyExact(julianDays, MILLIS_PER_DAY))
                .build();
            LocalDate localDate = LocalDate.of(
                utcCal.get(Calendar.YEAR),
                utcCal.get(Calendar.MONTH) + 1,
                // The number of days will be added later to handle non-existing
                // Julian dates in Proleptic Gregorian calendar.
                // For example, 1000-02-29 exists in Julian calendar because 1000
                // is a leap year but it is not a leap year in Gregorian calendar.
                1)
                .with(ChronoField.ERA, utcCal.get(Calendar.ERA))
                .plusDays(utcCal.get(Calendar.DAY_OF_MONTH) - 1);

            return Math.toIntExact(localDate.toEpochDay());
        } else if (obj instanceof LocalDate) {
            return Math.toIntExact(((LocalDate) obj).toEpochDay());
        }

        return null;
    }

    /**
     * Try to parse `like` to `equals`, `startsWith`, `endsWith`, `contains` if applicable.
     * @param columnPair Tuple2
     * @param children List
     * @return FilterPredicate or null
     */
    @Nullable
    private FilterPredicate like(
        Tuple2<Operators.Column, Comparable> columnPair,
        List<ResolvedExpression> children) {

        // check data types
        if (!(columnPair.f0 instanceof Operators.BinaryColumn)
            || !(columnPair.f1 instanceof Binary)) {
            return null;
        }

        // get expression value as String
        String likeValue = getStringValue(children);
        if (likeValue == null) {
            return null;
        }

        // return predicate
        String slicedValue;
        if (isPlainLikeValue(likeValue)) {
            return FilterApi.eq((Operators.BinaryColumn) columnPair.f0, (Binary) columnPair.f1);
        } else if ((slicedValue = getStartsWithFromLikeValue(likeValue)) != null) {
            return FilterApi.userDefined(columnPair.f0, new ApeStartWithFilter(slicedValue));
        } else if ((slicedValue = getEndsWithFromLikeValue(likeValue)) != null) {
            return FilterApi.userDefined(columnPair.f0, new ApeEndWithFilter(slicedValue));
        } else if ((slicedValue = getContainsWithFromLikeValue(likeValue)) != null) {
            return FilterApi.userDefined(columnPair.f0, new ApeContainsFilter(slicedValue));
        }

        return null;
    }

    @Nullable
    private String getStringValue(List<ResolvedExpression> children) {
        // check data type of expression value
        int valueIndex = 1;
        if (!literalOnRight(children)) {
            valueIndex = 0;
        }
        ValueLiteralExpression ex = (ValueLiteralExpression) children.get(valueIndex);
        DataType valueType = ex.getOutputDataType();

        // expression value must be able to cast to String
        if (valueType.getLogicalType().getTypeRoot() != LogicalTypeRoot.CHAR
            && valueType.getLogicalType().getTypeRoot() != LogicalTypeRoot.VARCHAR) {
            return null;
        }

        // get expression value as String
        return ex.getValueAs(String.class).orElse(null);
    }

    /**
     * Check whether a `like` pattern contains special characters.
     *
     * @param likeValue String
     * @return boolean true if there is no special character. For example, "abc19 1x"
     */
    private boolean isPlainLikeValue(String likeValue) {
        Pattern pattern = Pattern.compile("^[^%_!^\\[\\]\\\\]*$");
        return pattern.matcher(likeValue).matches();
    }

    /**
     * Try to get a `startsWith` value from a `like` pattern.
     * For example, "123%_" => "123". "123%abc%" => null.
     *
     * @param likeValue String
     * @return String the captured value contains no wildcards, or null if the pattern does not
     * just mean `startsWith`.
     */
    @Nullable
    private String getStartsWithFromLikeValue(String likeValue) {
        Pattern pattern = Pattern.compile("^([^%_!^\\[\\]\\\\]+)[%_]+$");
        Matcher matcher = pattern.matcher(likeValue);
        if (matcher.matches()) {
            return matcher.group(1);
        }

        return null;
    }

    /**
     * Try to get a `endsWith` value from a `like` pattern.
     * For example, "%123" => "123". "%123_abc" => null.
     *
     * @param likeValue String
     * @return String the captured value contains no wildcards, or null if the pattern does not
     * just mean `endsWith`.
     */
    @Nullable
    private String getEndsWithFromLikeValue(String likeValue) {
        Pattern pattern = Pattern.compile("^[%_]+([^%_!^\\[\\]\\\\]+)$");
        Matcher matcher = pattern.matcher(likeValue);
        if (matcher.matches()) {
            return matcher.group(1);
        }

        return null;
    }

    /**
     * Try to get a `contains` value from a `like` pattern.
     * For example, "%123%%" => "123". "%123%abc%" => null.
     *
     * @param likeValue String
     * @return String the captured value contains no wildcards, or null if the pattern does not
     *      * mean it `contains` extract one string.
     */
    @Nullable
    private String getContainsWithFromLikeValue(String likeValue) {
        Pattern pattern = Pattern.compile("^[%_]+([^%_!^\\[\\]\\\\]+)[%_]+$");
        Matcher matcher = pattern.matcher(likeValue);
        if (matcher.matches()) {
            return matcher.group(1);
        }

        return null;
    }

    @Override
    public FilterPredicate visit(ValueLiteralExpression valueLiteral) {

        return null;
    }

    @Override
    public FilterPredicate visit(FieldReferenceExpression fieldReference) {
        return null;
    }

    @Override
    public FilterPredicate visit(TypeLiteralExpression typeLiteral) {
        return null;
    }

    @Override
    public FilterPredicate visit(Expression other) {
        return null;
    }
}
