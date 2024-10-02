package org.byconity.paimon.util;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.byconity.paimon.PaimonMetaClient;
import org.byconity.paimon.PaimonTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;

public class RPNPredicateParserTest extends PaimonTestBase {

    private static Table table;

    @BeforeClass
    public static void beforeClass() throws Throwable {
        PaimonMetaClient client = PaimonMetaClient
                .create(GSON.toJson(createLocalParams()).getBytes(StandardCharsets.UTF_8));
        Catalog catalog = client.getCatalog();

        catalog.createDatabase("test_db", true);
        Identifier tableId = Identifier.create("test_db", "test_table");
        catalog.dropTable(tableId, true);

        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("col_int", DataTypes.INT());
        schemaBuilder.column("col_bigint", DataTypes.BIGINT());
        schemaBuilder.column("col_varchar", DataTypes.VARCHAR(16));
        schemaBuilder.column("col_decimal", DataTypes.DECIMAL(8, 2));
        schemaBuilder.column("col_double", DataTypes.DOUBLE());
        schemaBuilder.column("col_date", DataTypes.DATE());
        schemaBuilder.column("col_time", DataTypes.TIME());
        schemaBuilder.column("col_timestamp", DataTypes.TIMESTAMP());
        schemaBuilder.primaryKey("col_int", "col_bigint");
        schemaBuilder.partitionKeys("col_int");
        Schema schema = schemaBuilder.build();
        catalog.createTable(tableId, schema, false);

        table = catalog.getTable(tableId);
    }

    private boolean enableFailOnParseError() {
        try {
            Class<RPNPredicateParser> clazz = RPNPredicateParser.class;
            Field field = clazz.getDeclaredField("failOnParseError");
            field.setAccessible(true);
            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            boolean previous = (boolean) field.get(null);
            field.set(null, true);
            return previous;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void recoverFailOnParseError(boolean value) {
        try {
            Class<RPNPredicateParser> clazz = RPNPredicateParser.class;
            Field field = clazz.getDeclaredField("failOnParseError");
            field.setAccessible(true);
            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            field.set(null, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testEquals() {
        Predicate predicate = RPNPredicateParser.parse(table,
                "@function(equals,2)@identifier(col_int)@literal(Int64_-2118616973)");
        Assert.assertEquals(predicate.toString(), "Equal(col_int, -2118616973)");

        boolean previous = enableFailOnParseError();
        try {
            IllegalStateException illegalStateException = Assert
                    .assertThrows(IllegalStateException.class, () -> RPNPredicateParser.parse(table,
                            "@function(equals,2)@identifier(col_int)@identifier(col_bigint)"));
            Assert.assertEquals("checkNextTokenShouldBeLiteral failed",
                    illegalStateException.getMessage());
        } finally {
            recoverFailOnParseError(previous);
        }
    }

    @Test
    public void testNotEquals() {
        Predicate predicate = RPNPredicateParser.parse(table,
                "@function(notEquals,2)@identifier(col_varchar)@literal('MF8V2yvIvXD0')");
        Assert.assertEquals(predicate.toString(), "NotEqual(col_varchar, MF8V2yvIvXD0)");

        boolean previous = enableFailOnParseError();
        try {
            IllegalStateException illegalStateException = Assert
                    .assertThrows(IllegalStateException.class, () -> RPNPredicateParser.parse(table,
                            "@function(notEquals,2)@identifier(col_varchar)@identifier(col_varchar)"));
            Assert.assertEquals("checkNextTokenShouldBeLiteral failed",
                    illegalStateException.getMessage());
        } finally {
            recoverFailOnParseError(previous);
        }
    }

    @Test
    public void testLessThan() {
        Predicate predicate = RPNPredicateParser.parse(table,
                "@function(less,2)@identifier(col_decimal)@literal(Decimal32_'10.00')");
        Assert.assertEquals(predicate.toString(), "LessThan(col_decimal, 10.00)");

        boolean previous = enableFailOnParseError();
        try {
            IllegalStateException illegalStateException = Assert.assertThrows(
                    IllegalStateException.class,
                    () -> Assert.assertNull(RPNPredicateParser.parse(table,
                            "@function(less,2)@identifier(col_decimal)@identifier(col_decimal)")));
            Assert.assertEquals("checkNextTokenShouldBeLiteral failed",
                    illegalStateException.getMessage());
        } finally {
            recoverFailOnParseError(previous);
        }
    }

    @Test
    public void testLessOrEquals() {
        Predicate predicate = RPNPredicateParser.parse(table,
                "@function(lessOrEquals,2)@identifier(col_decimal)@literal(Float64_10.2)");
        Assert.assertEquals(predicate.toString(), "LessOrEqual(col_decimal, 10.2)");
        boolean previous = enableFailOnParseError();
        try {
            IllegalStateException illegalStateException = Assert.assertThrows(
                    IllegalStateException.class,
                    () -> Assert.assertNull(RPNPredicateParser.parse(table,
                            "@function(lessOrEquals,2)@identifier(col_decimal)@identifier(col_decimal)")));
            Assert.assertEquals("checkNextTokenShouldBeLiteral failed",
                    illegalStateException.getMessage());
        } finally {
            recoverFailOnParseError(previous);
        }
    }

    @Test
    public void testGreaterThan() {
        Predicate predicate = RPNPredicateParser.parse(table,
                "@function(greater,2)@identifier(col_int)@literal(UInt64_100)");
        Assert.assertEquals(predicate.toString(), "GreaterThan(col_int, 100)");
        boolean previous = enableFailOnParseError();
        try {
            IllegalStateException illegalStateException = Assert.assertThrows(
                    IllegalStateException.class,
                    () -> Assert.assertNull(RPNPredicateParser.parse(table,
                            "@function(greater,2)@identifier(col_int)@identifier(col_bigint)")));
            Assert.assertEquals("checkNextTokenShouldBeLiteral failed",
                    illegalStateException.getMessage());
        } finally {
            recoverFailOnParseError(previous);
        }
    }

    @Test
    public void testGreaterOrEquals() {
        Predicate predicate = RPNPredicateParser.parse(table,
                "@function(greaterOrEquals,2)@identifier(col_int)@literal(UInt64_100)");
        Assert.assertEquals(predicate.toString(), "GreaterOrEqual(col_int, 100)");
        boolean previous = enableFailOnParseError();
        try {
            IllegalStateException illegalStateException = Assert.assertThrows(
                    IllegalStateException.class,
                    () -> Assert.assertNull(RPNPredicateParser.parse(table,
                            "@function(greaterOrEquals,2)@identifier(col_int)@identifier(col_bigint)")));
            Assert.assertEquals("checkNextTokenShouldBeLiteral failed",
                    illegalStateException.getMessage());
        } finally {
            recoverFailOnParseError(previous);
        }
    }

    @Test
    public void testIn() {
        Predicate predicate = RPNPredicateParser.parse(table,
                "@function(in,2)@identifier(col_int)@function(tuple,3)@literal(Int64_1)@literal(Int64_2)@literal(Int64_3)");
        Assert.assertEquals(predicate.toString(),
                "Or([Or([Equal(col_int, 1), Equal(col_int, 2)]), Equal(col_int, 3)])");
        boolean previous = enableFailOnParseError();
        try {
            IllegalStateException illegalStateException = Assert.assertThrows(
                    IllegalStateException.class,
                    () -> Assert.assertNull(RPNPredicateParser.parse(table,
                            "@function(in,2)@identifier(col_int)@literal(Int64_1)@literal(Int64_2)@literal(Int64_3)")));
            Assert.assertEquals("checkNextTokenShouldBeTuple failed",
                    illegalStateException.getMessage());
        } finally {
            recoverFailOnParseError(previous);
        }
    }

    @Test
    public void testNotIn() {
        Predicate predicate = RPNPredicateParser.parse(table,
                "@function(notIn,2)@identifier(col_int)@function(tuple,3)@literal(Int64_1)@literal(Int64_2)@literal(Int64_3)");
        Assert.assertEquals(predicate.toString(),
                "And([And([NotEqual(col_int, 1), NotEqual(col_int, 2)]), NotEqual(col_int, 3)])");

        boolean previous = enableFailOnParseError();
        try {
            IllegalStateException illegalStateException = Assert.assertThrows(
                    IllegalStateException.class,
                    () -> Assert.assertNull(RPNPredicateParser.parse(table,
                            "@function(notIn,2)@identifier(col_int)@literal(Int64_1)@literal(Int64_2)@literal(Int64_3)")));
            Assert.assertEquals("checkNextTokenShouldBeTuple failed",
                    illegalStateException.getMessage());
        } finally {
            recoverFailOnParseError(previous);
        }
    }

    @Test
    public void testIsNull() {
        Predicate predicate =
                RPNPredicateParser.parse(table, "@function(isNull,1)@identifier(col_int)");
        Assert.assertEquals(predicate.toString(), "IsNull(col_int)");
    }

    @Test
    public void testIsNotNull() {
        Predicate predicate =
                RPNPredicateParser.parse(table, "@function(isNotNull,1)@identifier(col_int)");
        Assert.assertEquals(predicate.toString(), "IsNotNull(col_int)");
    }

    @Test
    public void testAndOr() {
        Predicate predicate = RPNPredicateParser.parse(table,
                "@function(and,2)@function(greater,2)@identifier(col_int)@literal(UInt64_1)@function(less,2)@identifier(col_int)@literal(UInt64_100)");
        Assert.assertEquals(predicate.toString(),
                "And([GreaterThan(col_int, 1), LessThan(col_int, 100)])");

        predicate = RPNPredicateParser.parse(table,
                "@function(or,2)@function(and,2)@function(greaterOrEquals,2)@identifier(col_int)@literal(Int64_-15)@function(lessOrEquals,2)@identifier(col_int)@literal(Int64_-1)@function(and,2)@function(greater,2)@identifier(col_int)@literal(Int64_1)@function(less,2)@identifier(col_int)@literal(Int64_100)");
        Assert.assertEquals(predicate.toString(),
                "Or([And([GreaterOrEqual(col_int, -15), LessOrEqual(col_int, -1)]), And([GreaterThan(col_int, 1), LessThan(col_int, 100)])])");

        predicate = RPNPredicateParser.parse(table,
                "@function(and,3)@function(greater,2)@identifier(col_int)@literal(UInt64_0)@function(greater,2)@identifier(col_bigint)@literal(UInt64_0)@function(greater,2)@identifier(col_varchar)@literal(UInt64_0)");
        Assert.assertEquals(predicate.toString(),
                "And([GreaterThan(col_varchar, 0), And([GreaterThan(col_int, 0), GreaterThan(col_bigint, 0)])])");

        predicate = RPNPredicateParser.parse(table,
                "@function(or,3)@function(greater,2)@identifier(col_int)@literal(UInt64_0)@function(greater,2)@identifier(col_bigint)@literal(UInt64_0)@function(greater,2)@identifier(col_varchar)@literal(UInt64_0)");
        Assert.assertEquals(predicate.toString(),
                "Or([GreaterThan(col_varchar, 0), Or([GreaterThan(col_int, 0), GreaterThan(col_bigint, 0)])])");
    }

    @Test
    public void testTime() {
        Predicate predicate = RPNPredicateParser.parse(table,
                "@function(and,3)@function(greater,2)@identifier(col_date)@function(cast,2)@literal(Int64_19357)@literal('Date32')@function(less,2)@identifier(col_time)@function(cast,2)@literal(UInt64_1719676826)@literal('DateTime')@function(less,2)@identifier(col_timestamp)@function(cast,2)@literal(Decimal64_'1728753747.255000')@literal('DateTime64(6)')");
        Assert.assertEquals(predicate.toString(),
                "And([LessThan(col_timestamp, 2024-10-12T17:22:27.255), And([GreaterThan(col_date, 19357), LessThan(col_time, 1719676826)])])");
    }

    @Test
    public void testLike() {
        Predicate predicate = RPNPredicateParser.parse(table,
                "@function(like,2)@identifier(col_varchar)@literal('hello%')");
        Assert.assertEquals(predicate.toString(), "StartsWith(col_varchar, hello)");

        predicate = RPNPredicateParser.parse(table,
                "@function(like,2)@identifier(col_varchar)@literal('%hello%')");
        Assert.assertNull(predicate);

        predicate = RPNPredicateParser.parse(table,
                "@function(like,2)@identifier(col_varchar)@literal('%hello')");
        Assert.assertNull(predicate);
    }

    @Test
    public void testComplex() {
        Predicate predicate = RPNPredicateParser.parse(table,
                "@function(and,5)@function(equals,2)@identifier(col_int)@literal(Int64_10)@function(in,2)@identifier(col_varchar)@function(tuple,2)@literal('value1')@literal('value2')@function(greater,2)@identifier(col_decimal)@literal(Decimal32_'100.50')@function(lessOrEquals,2)@identifier(col_decimal)@literal(Float64_200.75)@function(notIn,2)@identifier(col_decimal)@function(tuple,2)@literal(Decimal32_'300.00')@literal(Decimal32_'400.00')");
        Assert.assertEquals(predicate.toString(),
                "And([And([GreaterThan(col_decimal, 100.50), LessOrEqual(col_decimal, 200.75)]), And([And([NotEqual(col_decimal, 300.00), NotEqual(col_decimal, 400.00)]), And([Equal(col_int, 10), Or([Equal(col_varchar, value1), Equal(col_varchar, value2)])])])])");

        predicate = RPNPredicateParser.parse(table,
                "@function(and,4)@function(or,2)@function(equals,2)@identifier(col_int)@literal(UInt64_3)@function(greaterOrEquals,2)@identifier(col_date)@literal('2024-01-01')@function(or,2)@function(greater,2)@identifier(col_bigint)@literal(UInt64_8)@function(greaterOrEquals,2)@identifier(col_date)@literal('2024-01-01')@function(or,2)@function(equals,2)@identifier(col_int)@literal(UInt64_3)@function(lessOrEquals,2)@identifier(col_date)@literal('2024-01-02')@function(or,2)@function(greater,2)@identifier(col_bigint)@literal(UInt64_8)@function(lessOrEquals,2)@identifier(col_date)@literal('2024-01-02')");
        Assert.assertEquals(predicate.toString(),
                "And([And([Or([Equal(col_int, 3), GreaterOrEqual(col_date, 19723)]), Or([GreaterThan(col_bigint, 8), GreaterOrEqual(col_date, 19723)])]), And([Or([Equal(col_int, 3), LessOrEqual(col_date, 19724)]), Or([GreaterThan(col_bigint, 8), LessOrEqual(col_date, 19724)])])])");
    }

    @Test
    public void testFailover() {
        Predicate predicate = RPNPredicateParser.parse(table,
                "@function(and,2)@function(notEquals,2)@identifier(col_varchar)@literal('world')@function(like,2)@identifier(col_varchar)@literal('%he%')");
        Assert.assertEquals(predicate.toString(), "NotEqual(col_varchar, world)");

        predicate = RPNPredicateParser.parse(table,
                "@function(and,3)@function(equals,2)@identifier(col_int)@literal(Int64_10)@function(notEquals,2)@identifier(col_varchar)@literal('world')@function(like,2)@identifier(col_varchar)@literal('%he%')");
        Assert.assertEquals(predicate.toString(),
                "And([Equal(col_int, 10), NotEqual(col_varchar, world)])");

        predicate = RPNPredicateParser.parse(table,
                "@function(and,3)@function(equals,2)@identifier(col_int)@literal(Int64_10)@function(like,2)@identifier(col_varchar)@literal('%he%')@function(notEquals,2)@identifier(col_varchar)@literal('world')");
        Assert.assertEquals(predicate.toString(),
                "And([Equal(col_int, 10), NotEqual(col_varchar, world)])");

        predicate = RPNPredicateParser.parse(table,
                "@function(and,3)@function(like,2)@identifier(col_varchar)@literal('%he%')@function(equals,2)@identifier(col_int)@literal(Int64_10)@function(notEquals,2)@identifier(col_varchar)@literal('world')");
        Assert.assertEquals(predicate.toString(),
                "And([Equal(col_int, 10), NotEqual(col_varchar, world)])");

        predicate = RPNPredicateParser.parse(table,
                "@function(and,2)@function(equals,2)@identifier(col_int)@literal(Int64_10)@function(and,3)@function(like,2)@identifier(col_varchar)@literal('%he%')@function(equals,2)@identifier(col_int)@literal(Int64_10)@function(notEquals,2)@identifier(col_varchar)@literal('world')");
        Assert.assertEquals(predicate.toString(), "Equal(col_int, 10)");

        predicate = RPNPredicateParser.parse(table,
                "@function(and,2)@function(and,3)@function(like,2)@identifier(col_varchar)@literal('%he%')@function(equals,2)@identifier(col_int)@literal(Int64_10)@function(notEquals,2)@identifier(col_varchar)@literal('world')@function(equals,2)@identifier(col_int)@literal(Int64_10)");
        Assert.assertEquals(predicate.toString(), "Equal(col_int, 10)");
    }
}
