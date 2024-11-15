package org.byconity.iceberg.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.byconity.common.util.AbstractRPNPredicateParser;
import org.byconity.common.util.ExceptionUtils;
import org.byconity.common.util.TimeUtils;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class IcebergRPNPredicateParser
        extends AbstractRPNPredicateParser<Table, Types.NestedField, Expression> {

    private final Map<String, Integer> columnNameToIdx = Maps.newHashMap();

    public static Expression parse(Table table, String content) {
        if (content == null) {
            return Expressions.alwaysTrue();
        }
        try {
            IcebergRPNPredicateParser parser = new IcebergRPNPredicateParser(table, content);
            return parser.parse();
        } catch (Exception e) {
            LOGGER.error("Fail to parse rpn_predicate={}, msg={}", content, e.getMessage(), e);
            if (failOnParseError) {
                ExceptionUtils.rethrow(e);
            }
        }
        return Expressions.alwaysTrue();
    }

    private IcebergRPNPredicateParser(Table table, String content) {
        super(table, content);
        int size = table.schema().columns().size();
        for (int i = 0; i < size; i++) {
            Types.NestedField dataField = table.schema().columns().get(i);
            columnNameToIdx.put(dataField.name(), i);
        }
    }

    @Override
    protected Types.NestedField checkAndGetColumnType(String columnName) {
        Integer idx = columnNameToIdx.get(columnName);
        Preconditions.checkState(idx != null, "Cannot find column '" + columnName + "'");
        return table.schema().columns().get(idx);
    }

    @Override
    protected Expression buildAnd(Expression left, Expression right) {
        return Expressions.and(left, right);
    }

    @Override
    protected Expression buildOr(Expression left, Expression right) {
        return Expressions.or(left, right);
    }

    @Override
    protected Expression buildEqual(String columnName, Object value) {
        return Expressions.equal(columnName, value);
    }

    @Override
    protected Expression buildNotEqual(String columnName, Object value) {
        return Expressions.notEqual(columnName, value);
    }

    @Override
    protected Expression buildLessThan(String columnName, Object value) {
        return Expressions.lessThan(columnName, value);
    }

    @Override
    protected Expression buildLessOrEqual(String columnName, Object value) {
        return Expressions.lessThanOrEqual(columnName, value);
    }

    @Override
    protected Expression buildGreaterThan(String columnName, Object value) {
        return Expressions.greaterThan(columnName, value);
    }

    @Override
    protected Expression buildGreaterOrEqual(String columnName, Object value) {
        return Expressions.greaterThanOrEqual(columnName, value);
    }

    @Override
    protected Expression buildIn(String columnName, List<Object> values) {
        return Expressions.in(columnName, values);
    }

    @Override
    protected Expression buildNotIn(String columnName, List<Object> values) {
        return Expressions.notIn(columnName, values);
    }

    @Override
    protected Expression buildIsNull(String columnName) {
        return Expressions.isNull(columnName);
    }

    @Override
    protected Expression buildIsNotNull(String columnName) {
        return Expressions.notNull(columnName);
    }

    @Override
    protected Expression buildStartsWith(String columnName, Object value) {
        String expr = ((String) value);
        Preconditions.checkState(!expr.startsWith("%") && expr.endsWith("%"));
        return Expressions.startsWith(columnName, expr.substring(0, expr.length() - 1));
    }

    protected Object cast(String type, Object value) {
        // type is something like
        // 1. Date
        // 1. Date32
        // 2. DateTime
        // 3. DateTime64(6)
        type = type.replaceAll("\\([0-9]+\\)", "");
        switch (type) {
            case "Date":
            case "Date32": {
                Preconditions.checkState(value instanceof Number);
                return ((Number) value).intValue();
            }
            case "DateTime64": {
                final long epochSecond;
                final long nanoAdjustment;
                // Number type for precision 0
                // Decimal type for precision > 0
                if (value instanceof Number && !(value instanceof BigDecimal)) {
                    final long milliseconds = ((Number) value).longValue();

                    epochSecond = milliseconds / 1000;
                    nanoAdjustment = milliseconds % 1000 * 1_000_000;
                } else if (value instanceof BigDecimal) {
                    // Example 1712795208.976000
                    // 1712795208 is seconds
                    final BigDecimal decimalInSeconds = ((BigDecimal) value);

                    epochSecond = decimalInSeconds.toBigInteger().longValue();
                    nanoAdjustment = decimalInSeconds.remainder(BigDecimal.ONE)
                            .multiply(BigDecimal.valueOf(1_000_000_000)).longValue();
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported value type for DateTime64");
                }

                return LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSecond, nanoAdjustment),
                        TimeUtils.ZONE_UTC);
            }
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported cast type '%s'.", type));
        }
    }

    protected Object visitLiteral(String[] tokenArgs) {
        Preconditions.checkState(tokenArgs.length == 1);
        final String literalTypeAndValue = tokenArgs[0];
        LiteralType literalType = null;
        String literalValue = null;
        for (LiteralType type : LiteralType.values()) {
            if (literalTypeAndValue.startsWith(type.prefix)) {
                literalType = type;
                literalValue = literalTypeAndValue.substring(type.prefix.length());
                break;
            }
        }
        // String has no type prefix
        if (literalType == null) {
            literalValue = literalTypeAndValue;
            if (literalValue.startsWith("'") && literalValue.endsWith("'")
                    || literalValue.startsWith("\"") && literalValue.endsWith("\"")) {
                literalValue = literalValue.substring(1, literalValue.length() - 1);
            }
            if (recentIdentifierType == null) {
                // For cases like parse second params of cast function
                return literalValue;
            }
            switch (recentIdentifierType.type().typeId()) {
                case STRING:
                    return literalValue;
                case FIXED: {
                    Types.FixedType fixedType = (Types.FixedType) recentIdentifierType.type();
                    byte[] sourceBytes = literalValue.getBytes(Charset.defaultCharset());
                    byte[] bytes = new byte[fixedType.length()];
                    System.arraycopy(sourceBytes, 0, bytes, 0, sourceBytes.length);
                    return bytes;
                }
                case BINARY:
                    return literalValue.getBytes(StandardCharsets.UTF_8);
                case DATE: {
                    for (DateTimeFormatter datePattern : TimeUtils.DATE_PATTERNS) {
                        try {
                            return LocalDate.parse(literalValue, datePattern);
                        } catch (Exception e) {
                            // try next pattern
                        }
                    }
                    throw new UnsupportedOperationException(
                            String.format("Unsupported date pattern, %s", literalValue));
                }
                case TIMESTAMP: {
                    int precision = ((Types.DecimalType) recentIdentifierType.type()).precision();
                    return LocalDateTime.parse(literalValue,
                            TimeUtils.DATETIME_PATTERNS.get(precision));
                }
                default:
                    throw new UnsupportedOperationException(String.format(
                            "Unsupported auto string literal cast to type iceberg TypeId '%s'",
                            recentIdentifierType.type().typeId().name()));
            }
        }

        LOGGER.debug("literalTypeAndType={}, literalValue={}", literalTypeAndValue, literalValue);

        switch (literalType) {
            case NULL:
                return null;
            case NegativeInfinity:
                if (recentIdentifierType.type() instanceof Types.FloatType) {
                    return Float.NEGATIVE_INFINITY;
                } else {
                    return Double.NEGATIVE_INFINITY;
                }
            case PositiveInfinity:
                if (recentIdentifierType.type() instanceof Types.FloatType) {
                    return Float.POSITIVE_INFINITY;
                } else {
                    return Double.POSITIVE_INFINITY;
                }
            case Int64:
            case Int128:
            case Int256:
            case UInt64:
            case UInt128:
            case UInt256: {
                if (recentIdentifierType.type() instanceof Types.BooleanType) {
                    return Integer.parseInt(literalValue) != 0;
                } else if (recentIdentifierType.type() instanceof Types.IntegerType) {
                    return Integer.parseInt(literalValue);
                } else {
                    return Long.parseLong(literalValue);
                }
            }
            case Float64: {
                if (recentIdentifierType.type() instanceof Types.FloatType) {
                    return Float.parseFloat(literalValue);
                } else {
                    return Double.parseDouble(literalValue);
                }
            }
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal256: {
                // Remove around single quote
                Preconditions.checkState(literalValue.length() >= 2);
                literalValue = literalValue.substring(1, literalValue.length() - 1);
                BigDecimal decimal = new BigDecimal(literalValue);
                if (recentIdentifierType.type() instanceof Types.DecimalType) {
                    Types.DecimalType decimalType = (Types.DecimalType) recentIdentifierType.type();
                    int precision = (decimalType).precision();
                    int scale = (decimalType).scale();
                    return new BigDecimal(literalValue,
                            new MathContext(precision, RoundingMode.HALF_UP)).setScale(scale,
                                    RoundingMode.HALF_UP);
                } else {
                    return decimal;
                }
            }
            case UUID:
            case IPv4:
            case IPv6:
                return literalValue;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported literal type='%s'", literalType));
        }
    }
}
