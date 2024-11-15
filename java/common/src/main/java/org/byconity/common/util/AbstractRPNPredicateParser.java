package org.byconity.common.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Queue;

public abstract class AbstractRPNPredicateParser<Table, Type, Predicate> {
    protected static final Logger LOGGER =
            LoggerFactory.getLogger(AbstractRPNPredicateParser.class);
    protected static final boolean failOnParseError;
    protected final Table table;
    protected final String content;
    protected int cursor = 0;
    protected Type recentIdentifierType;
    protected int parseLevel;

    static {
        String value = System.getenv("JNI_PREDICATE_FAIL_ON_PARSE_ERROR");
        failOnParseError = value != null;
    }

    protected AbstractRPNPredicateParser(Table table, String content) {
        this.table = table;
        this.content = content;
    }

    protected final Predicate parse() {
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
        Token token = Objects.requireNonNull(nextToken());

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
                    predicates.offer(buildAnd(first, second));
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
                    predicates.offer(buildOr(first, second));
                }
                return predicates.poll();
            }
            case equals: {
                String columnName = visit();
                recentIdentifierType = checkAndGetColumnType(columnName);
                checkNextTokenShouldBeLiteral();
                return buildEqual(columnName, visit());
            }
            case notEquals: {
                String columnName = visit();
                recentIdentifierType = checkAndGetColumnType(columnName);
                checkNextTokenShouldBeLiteral();
                return buildNotEqual(columnName, visit());
            }
            case less: {
                String columnName = visit();
                recentIdentifierType = checkAndGetColumnType(columnName);
                checkNextTokenShouldBeLiteral();
                return buildLessThan(columnName, visit());
            }
            case lessOrEquals: {
                String columnName = visit();
                recentIdentifierType = checkAndGetColumnType(columnName);
                checkNextTokenShouldBeLiteral();
                return buildLessOrEqual(columnName, visit());
            }
            case greater: {
                String columnName = visit();
                recentIdentifierType = checkAndGetColumnType(columnName);
                checkNextTokenShouldBeLiteral();
                return buildGreaterThan(columnName, visit());
            }
            case greaterOrEquals: {
                String columnName = visit();
                recentIdentifierType = checkAndGetColumnType(columnName);
                checkNextTokenShouldBeLiteral();
                return buildGreaterOrEqual(columnName, visit());
            }
            case in: {
                String columnName = visit();
                recentIdentifierType = checkAndGetColumnType(columnName);
                checkNextTokenShouldBeTuple();
                return buildIn(columnName, visit());
            }
            case notIn: {
                String columnName = visit();
                recentIdentifierType = checkAndGetColumnType(columnName);
                checkNextTokenShouldBeTuple();
                return buildNotIn(columnName, visit());
            }
            case isNull: {
                String columnName = visit();
                recentIdentifierType = checkAndGetColumnType(columnName);
                return buildIsNull(columnName);
            }
            case isNotNull: {
                String columnName = visit();
                recentIdentifierType = checkAndGetColumnType(columnName);
                return buildIsNotNull(columnName);
            }
            case like: {
                String columnName = visit();
                recentIdentifierType = checkAndGetColumnType(columnName);
                checkNextTokenShouldBeLiteral();
                return buildStartsWith(columnName, visit());
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

    abstract protected Object cast(String type, Object value);

    private String visitIdentifier(String[] tokenValues) {
        Preconditions.checkState(tokenValues.length == 1);
        return tokenValues[0];
    }

    abstract protected Object visitLiteral(String[] tokenArgs);

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
    protected enum LiteralType {
        NULL("NULL"), NegativeInfinity("-Inf"), PositiveInfinity("+Inf"), UInt64(
                "UInt64_"), UInt128("UInt128_"), UInt256("UInt256_"), Int64("Int64_"), Int128(
                        "Int128_"), Int256("Int256_"), UUID("UUID_"), IPv4("IPv4_"), IPv6(
                                "IPv6_"), Float64("Float64_"), Decimal32("Decimal32_"), Decimal64(
                                        "Decimal64_"), Decimal128(
                                                "Decimal128_"), Decimal256("Decimal256_"),;

        LiteralType(String prefix) {
            this.prefix = prefix;
        }

        public final String prefix;
    }

    abstract protected Type checkAndGetColumnType(String columnName);

    abstract protected Predicate buildAnd(Predicate left, Predicate right);

    abstract protected Predicate buildOr(Predicate left, Predicate right);

    abstract protected Predicate buildEqual(String columnName, Object value);

    abstract protected Predicate buildNotEqual(String columnName, Object value);

    abstract protected Predicate buildLessThan(String columnName, Object value);

    abstract protected Predicate buildLessOrEqual(String columnName, Object value);

    abstract protected Predicate buildGreaterThan(String columnName, Object value);

    abstract protected Predicate buildGreaterOrEqual(String columnName, Object value);

    abstract protected Predicate buildIn(String columnName, List<Object> values);

    abstract protected Predicate buildNotIn(String columnName, List<Object> values);

    abstract protected Predicate buildIsNull(String columnName);

    abstract protected Predicate buildIsNotNull(String columnName);

    abstract protected Predicate buildStartsWith(String columnName, Object value);
}
