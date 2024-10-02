package org.byconity.paimon.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.*;
import org.byconity.common.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.Queue;

public class RPNPredicateParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(RPNPredicateParser.class);
    private static final boolean failOnParseError;
    private final Table table;
    private final List<String> fieldNames;
    private final String content;
    private int cursor = 0;
    private DataType recentIdentifierType;
    private int parseLevel;

    static {
        String value = System.getenv("JNI_PAIMON_PREDICATE_FAIL_ON_PARSE_ERROR");
        failOnParseError = value != null;
    }

    public static Predicate parse(Table table, String content) {
        if (content == null) {
            return null;
        }
        try {
            RPNPredicateParser parser = new RPNPredicateParser(table, content);
            return parser.parse();
        } catch (Exception e) {
            LOGGER.error("Fail to parse rpn_predicate={}, msg={}", content, e.getMessage(), e);
            if (failOnParseError) {
                ExceptionUtils.rethrow(e);
            }
        }
        return null;
    }

    private RPNPredicateParser(Table table, String content) {
        this.table = table;
        this.fieldNames = PaimonUtils.fieldNames(this.table.rowType());
        this.content = content;
    }

    private Predicate parse() {
        return visit();
    }

    // This method try to look ahead next token, but won't truely forward the cursor
    private Token lookAheadNextToken() {
        int savedCursor = cursor;
        Token token = nextToken();
        cursor = savedCursor;
        return token;
    }

    private Token nextToken() {
        if (cursor == content.length()) {
            return null;
        }
        Preconditions.checkState(content.charAt(cursor) == '@');
        int leftParenthesis = content.indexOf('(', cursor);
        TokenType tokenType = TokenType.valueOf(content.substring(cursor + 1, leftParenthesis));

        cursor = leftParenthesis + 1;
        int leftParenthesisCnt = 0;
        while (cursor < content.length()) {
            char c = content.charAt(cursor);
            if (c == '(') {
                leftParenthesisCnt++;
            } else if (c == ')') {
                if (leftParenthesisCnt == 0) {
                    break;
                }
                leftParenthesisCnt--;
            }
            cursor++;
        }

        String[] args = content.substring(leftParenthesis + 1, cursor).split(",");
        cursor++;
        return new Token(tokenType, args);
    }

    @SuppressWarnings("unchecked")
    private <T> T visit() {
        try {
            parseLevel++;
            Token token;
            if ((token = nextToken()) == null) {
                return null;
            }

            switch (token.type) {
                case function:
                    return (T) visitFunction(token.args);
                case identifier:
                    return (T) visitIdentifier(token.args);
                case literal:
                    return (T) visitLiteral(token.args);
                default:
                    throw new UnsupportedOperationException();
            }
        } finally {
            parseLevel--;
        }
    }

    private void skip(int backupCursor) {
        cursor = backupCursor;
        Token token = nextToken();
        Preconditions.checkNotNull(token);

        int remainingTokens = 0;

        while (true) {
            switch (token.type) {
                case function: {
                    Preconditions.checkState(token.args.length == 2);
                    int argNum = Integer.parseInt(token.args[1]);
                    remainingTokens += argNum;
                    break;
                }
                case identifier:
                case literal:
                    break;
            }
            if (remainingTokens == 0) {
                break;
            }
            token = nextToken();
            remainingTokens--;
        }
    }

    private void checkNextTokenShouldBeLiteral() {
        Token nextToken = lookAheadNextToken();
        Preconditions.checkNotNull(nextToken);
        switch (nextToken.type) {
            case literal:
                return;
            case function:
                FunctionType functionType = FunctionType.valueOf(nextToken.args[0]);
                if (functionType == FunctionType.cast) {
                    return;
                }
        }
        throw new IllegalStateException("checkNextTokenShouldBeLiteral failed");
    }

    private void checkNextTokenShouldBeTuple() {
        Token nextToken = lookAheadNextToken();
        Preconditions.checkNotNull(nextToken);
        if (Objects.requireNonNull(nextToken.type) == TokenType.function) {
            FunctionType functionType = FunctionType.valueOf(nextToken.args[0]);
            if (functionType == FunctionType.tuple) {
                return;
            }
        }
        throw new IllegalStateException("checkNextTokenShouldBeTuple failed");
    }

    private Object visitFunction(String[] tokenValues) {
        Preconditions.checkState(tokenValues.length == 2);
        FunctionType functionType = FunctionType.valueOf(tokenValues[0]);
        int argNum = Integer.parseInt(tokenValues[1]);
        Preconditions.checkState(functionType.isVararg() || functionType.argNum == argNum);

        PredicateBuilder builder = new PredicateBuilder(table.rowType());

        switch (functionType) {
            case tuple: {
                List<Object> items = Lists.newArrayList();
                for (int i = 0; i < argNum; i++) {
                    checkNextTokenShouldBeLiteral();
                    items.add(visit());
                }
                return items;
            }
            case and: {
                Queue<Predicate> predicates = Lists.newLinkedList();
                boolean isTopLevelAnd = (parseLevel == 1);
                for (int i = 0; i < argNum; i++) {
                    int backupCursor = cursor;
                    try {
                        predicates.offer(visit());
                    } catch (Exception e) {
                        if (failOnParseError || !isTopLevelAnd) {
                            throw e;
                        }
                        skip(backupCursor);
                    }
                }
                while (predicates.size() > 1) {
                    Predicate first = predicates.poll();
                    Predicate second = predicates.poll();
                    predicates.offer(PredicateBuilder.and(first, second));
                }
                return predicates.poll();
            }
            case or: {
                Queue<Predicate> predicates = Lists.newLinkedList();
                for (int i = 0; i < argNum; i++) {
                    predicates.offer(visit());
                }
                while (predicates.size() > 1) {
                    Predicate first = predicates.poll();
                    Predicate second = predicates.poll();
                    predicates.offer(PredicateBuilder.or(first, second));
                }
                return predicates.poll();
            }
            case equals: {
                String columnName = visit();
                int index = fieldNames.indexOf(columnName);
                Preconditions.checkState(index != -1, "Cannot find column '" + columnName + "'");
                recentIdentifierType = table.rowType().getTypeAt(index);
                checkNextTokenShouldBeLiteral();
                return builder.equal(index, visit());
            }
            case notEquals: {
                String columnName = visit();
                int index = fieldNames.indexOf(columnName);
                Preconditions.checkState(index != -1, "Cannot find column '" + columnName + "'");
                recentIdentifierType = table.rowType().getTypeAt(index);
                checkNextTokenShouldBeLiteral();
                return builder.notEqual(index, visit());
            }
            case less: {
                String columnName = visit();
                int index = fieldNames.indexOf(columnName);
                Preconditions.checkState(index != -1, "Cannot find column '" + columnName + "'");
                recentIdentifierType = table.rowType().getTypeAt(index);
                checkNextTokenShouldBeLiteral();
                return builder.lessThan(index, visit());
            }
            case lessOrEquals: {
                String columnName = visit();
                int index = fieldNames.indexOf(columnName);
                Preconditions.checkState(index != -1, "Cannot find column '" + columnName + "'");
                recentIdentifierType = table.rowType().getTypeAt(index);
                checkNextTokenShouldBeLiteral();
                return builder.lessOrEqual(index, visit());
            }
            case greater: {
                String columnName = visit();
                int index = fieldNames.indexOf(columnName);
                Preconditions.checkState(index != -1, "Cannot find column '" + columnName + "'");
                recentIdentifierType = table.rowType().getTypeAt(index);
                checkNextTokenShouldBeLiteral();
                return builder.greaterThan(index, visit());
            }
            case greaterOrEquals: {
                String columnName = visit();
                int index = fieldNames.indexOf(columnName);
                Preconditions.checkState(index != -1, "Cannot find column '" + columnName + "'");
                recentIdentifierType = table.rowType().getTypeAt(index);
                checkNextTokenShouldBeLiteral();
                return builder.greaterOrEqual(index, visit());
            }
            case in: {
                String columnName = visit();
                int index = fieldNames.indexOf(columnName);
                Preconditions.checkState(index != -1, "Cannot find column '" + columnName + "'");
                recentIdentifierType = table.rowType().getTypeAt(index);
                checkNextTokenShouldBeTuple();
                return builder.in(index, visit());
            }
            case notIn: {
                String columnName = visit();
                int index = fieldNames.indexOf(columnName);
                Preconditions.checkState(index != -1, "Cannot find column '" + columnName + "'");
                recentIdentifierType = table.rowType().getTypeAt(index);
                checkNextTokenShouldBeTuple();
                return builder.notIn(index, visit());
            }
            case isNull: {
                String columnName = visit();
                int index = fieldNames.indexOf(columnName);
                Preconditions.checkState(index != -1, "Cannot find column '" + columnName + "'");
                return builder.isNull(index);
            }
            case isNotNull: {
                String columnName = visit();
                int index = fieldNames.indexOf(columnName);
                Preconditions.checkState(index != -1, "Cannot find column '" + columnName + "'");
                return builder.isNotNull(index);
            }
            case like: {
                String columnName = visit();
                int index = fieldNames.indexOf(columnName);
                Preconditions.checkState(index != -1, "Cannot find column '" + columnName + "'");
                recentIdentifierType = table.rowType().getTypeAt(index);
                checkNextTokenShouldBeLiteral();
                String expr = ((BinaryString) Objects.requireNonNull(visit())).toString();
                Preconditions.checkState(!expr.startsWith("%") && expr.endsWith("%"));
                return builder.startsWith(index,
                        BinaryString.fromString(expr.substring(0, expr.length() - 1)));
            }
            case cast: {
                checkNextTokenShouldBeLiteral();
                Object value = visit();
                // reset recentIdentifierType for correctly parsing cast to type
                recentIdentifierType = null;
                String type = Objects.requireNonNull(visit());
                return cast(type, value);
            }
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported function '%s'.", functionType));
        }
    }

    private static Object cast(String type, Object value) {
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

    private String visitIdentifier(String[] tokenValues) {
        Preconditions.checkState(tokenValues.length == 1);
        return tokenValues[0];
    }

    private Object visitLiteral(String[] tokenArgs) {
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

    private static final class Token {
        private final TokenType type;
        private final String[] args;

        private Token(TokenType type, String[] args) {
            this.type = type;
            this.args = args;
        }
    }


    enum TokenType {
        function, identifier, literal;
    }


    enum FunctionType {
        tuple(-1), and(-1), or(-1), equals(2), notEquals(2), less(2), lessOrEquals(2), greater(
                2), greaterOrEquals(2), in(2), notIn(2), isNull(1), isNotNull(1), like(2), cast(2);

        FunctionType(int argNum) {
            this.argNum = argNum;
        }

        final int argNum;

        boolean isVararg() {
            return argNum == -1;
        }
    }


    // Type prefix refers to src/Common/FieldVisitorDump.h
    enum LiteralType {
        NULL("NULL"), NegativeInfinity("-Inf"), PositiveInfinity("+Inf"), UInt64(
                "UInt64_"), UInt128("UInt128_"), UInt256("UInt256_"), Int64("Int64_"), Int128(
                        "Int128_"), Int256("Int256_"), UUID("UUID_"), IPv4("IPv4_"), IPv6(
                                "IPv6_"), Float64("Float64_"), Decimal32("Decimal32_"), Decimal64(
                                        "Decimal64_"), Decimal128(
                                                "Decimal128_"), Decimal256("Decimal256_"),;

        LiteralType(String prefix) {
            this.prefix = prefix;
        }

        final String prefix;
    }
}
