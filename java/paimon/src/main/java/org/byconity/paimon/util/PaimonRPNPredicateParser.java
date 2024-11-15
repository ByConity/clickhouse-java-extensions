package org.byconity.paimon.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.*;
import org.byconity.common.util.AbstractRPNPredicateParser;
import org.byconity.common.util.ExceptionUtils;
import org.byconity.common.util.TimeUtils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class PaimonRPNPredicateParser
        extends AbstractRPNPredicateParser<Table, DataType, Predicate> {

    private final Map<String, Integer> columnNameToIdx = Maps.newHashMap();

    public static Predicate parse(Table table, String content) {
        if (content == null) {
            return null;
        }
        try {
            PaimonRPNPredicateParser parser = new PaimonRPNPredicateParser(table, content);
            return parser.parse();
        } catch (Exception e) {
            LOGGER.error("Fail to parse rpn_predicate={}, msg={}", content, e.getMessage(), e);
            if (failOnParseError) {
                ExceptionUtils.rethrow(e);
            }
        }
        return null;
    }

    private PaimonRPNPredicateParser(Table table, String content) {
        super(table, content);
        int size = table.rowType().getFields().size();
        for (int i = 0; i < size; i++) {
            DataField dataField = table.rowType().getFields().get(i);
            columnNameToIdx.put(dataField.name(), i);
        }
    }

    private PredicateBuilder builder() {
        return new PredicateBuilder(table.rowType());
    }

    @Override
    protected DataType checkAndGetColumnType(String columnName) {
        Integer idx = columnNameToIdx.get(columnName);
        Preconditions.checkState(idx != null, "Cannot find column '" + columnName + "'");
        return table.rowType().getTypeAt(idx);
    }

    @Override
    protected Predicate buildAnd(Predicate left, Predicate right) {
        return PredicateBuilder.and(left, right);
    }

    @Override
    protected Predicate buildOr(Predicate left, Predicate right) {
        return PredicateBuilder.or(left, right);
    }

    @Override
    protected Predicate buildEqual(String columnName, Object value) {
        return builder().equal(columnNameToIdx.get(columnName), value);
    }

    @Override
    protected Predicate buildNotEqual(String columnName, Object value) {
        return builder().notEqual(columnNameToIdx.get(columnName), value);
    }

    @Override
    protected Predicate buildLessThan(String columnName, Object value) {
        return builder().lessThan(columnNameToIdx.get(columnName), value);
    }

    @Override
    protected Predicate buildLessOrEqual(String columnName, Object value) {
        return builder().lessOrEqual(columnNameToIdx.get(columnName), value);
    }

    @Override
    protected Predicate buildGreaterThan(String columnName, Object value) {
        return builder().greaterThan(columnNameToIdx.get(columnName), value);
    }

    @Override
    protected Predicate buildGreaterOrEqual(String columnName, Object value) {
        return builder().greaterOrEqual(columnNameToIdx.get(columnName), value);
    }

    @Override
    protected Predicate buildIn(String columnName, List<Object> values) {
        return builder().in(columnNameToIdx.get(columnName), values);
    }

    @Override
    protected Predicate buildNotIn(String columnName, List<Object> values) {
        return builder().notIn(columnNameToIdx.get(columnName), values);
    }

    @Override
    protected Predicate buildIsNull(String columnName) {
        return builder().isNull(columnNameToIdx.get(columnName));
    }

    @Override
    protected Predicate buildIsNotNull(String columnName) {
        return builder().isNotNull(columnNameToIdx.get(columnName));
    }

    @Override
    protected Predicate buildStartsWith(String columnName, Object value) {
        String expr = ((BinaryString) value).toString();
        Preconditions.checkState(!expr.startsWith("%") && expr.endsWith("%"));
        return builder().startsWith(columnNameToIdx.get(columnName),
                BinaryString.fromString(expr.substring(0, expr.length() - 1)));
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
            case "DateTime": {
                // Clickhouse's DateTime maps to Paimon's TimeType, where time type has precision.
                // But Clickhouse doesn't support DateTime with precision (resolution must be 1
                // second).
                Preconditions.checkState(value instanceof Number,
                        "Unsupported value type for DateTime");
                return ((Number) value).intValue();
            }
            case "DateTime64": {
                final long epochSecond;
                final long nanoAdjustment;
                // Number type for precision 0
                // Decimal type for precision > 0
                if (value instanceof Number) {
                    final long milliseconds = ((Number) value).longValue();

                    epochSecond = milliseconds / 1000;
                    nanoAdjustment = milliseconds % 1000 * 1_000_000;
                } else if (value instanceof Decimal) {
                    // Example 1712795208.976000
                    // 1712795208 is seconds
                    final BigDecimal decimalInSeconds = ((Decimal) value).toBigDecimal();

                    epochSecond = decimalInSeconds.toBigInteger().longValue();
                    nanoAdjustment = decimalInSeconds.remainder(BigDecimal.ONE)
                            .multiply(BigDecimal.valueOf(1_000_000_000)).longValue();
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported value type for DateTime64");
                }

                return Timestamp.fromInstant(Instant.ofEpochSecond(epochSecond, nanoAdjustment));
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
            switch (recentIdentifierType.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return BinaryString.fromString(literalValue);
                case BINARY:
                case VARBINARY:
                    return literalValue.getBytes(StandardCharsets.UTF_8);
                case DATE: {
                    for (DateTimeFormatter datePattern : TimeUtils.DATE_PATTERNS) {
                        try {
                            LocalDate localDate = LocalDate.parse(literalValue, datePattern);
                            return (int) localDate.toEpochDay();
                        } catch (Exception e) {
                            // try next pattern
                        }
                    }
                    throw new UnsupportedOperationException(
                            String.format("Unsupported date pattern, %s", literalValue));
                }
                case TIMESTAMP_WITHOUT_TIME_ZONE: {
                    int precision = ((TimestampType) recentIdentifierType).getPrecision();
                    return Timestamp.fromInstant(LocalDateTime
                            .parse(literalValue, TimeUtils.DATETIME_PATTERNS.get(precision))
                            .toInstant(TimeUtils.DEFAULT_ZONE_OFFSET));
                }
                default:
                    throw new UnsupportedOperationException(String.format(
                            "Unsupported auto string literal cast to type paimon TypeRoot '%s'",
                            recentIdentifierType.getTypeRoot().name()));
            }
        }

        LOGGER.debug("literalTypeAndType={}, literalValue={}", literalTypeAndValue, literalValue);

        switch (literalType) {
            case NULL:
                return null;
            case NegativeInfinity:
                if (recentIdentifierType instanceof FloatType) {
                    return Float.NEGATIVE_INFINITY;
                } else {
                    return Double.NEGATIVE_INFINITY;
                }
            case PositiveInfinity:
                if (recentIdentifierType instanceof FloatType) {
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
                if (recentIdentifierType instanceof BooleanType) {
                    return Integer.parseInt(literalValue) != 0;
                } else if (recentIdentifierType instanceof TinyIntType) {
                    return Byte.parseByte(literalValue);
                } else if (recentIdentifierType instanceof SmallIntType) {
                    return Short.parseShort(literalValue);
                } else if (recentIdentifierType instanceof IntType) {
                    return Integer.parseInt(literalValue);
                } else {
                    return Long.parseLong(literalValue);
                }
            }
            case Float64: {
                if (recentIdentifierType instanceof FloatType) {
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
                if (recentIdentifierType instanceof DecimalType) {
                    DecimalType decimalType = (DecimalType) recentIdentifierType;
                    return Decimal.fromBigDecimal(decimal, decimalType.getPrecision(),
                            decimalType.getScale());
                } else {
                    return Decimal.fromBigDecimal(decimal, decimal.precision(), decimal.scale());
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
