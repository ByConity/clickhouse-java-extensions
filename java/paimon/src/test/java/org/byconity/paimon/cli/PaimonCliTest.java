package org.byconity.paimon.cli;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class PaimonCliTest extends PaimonCliTestBase {

    @Test
    public void help() {
        Assert.assertEquals(helpMessage, "usage: PaimonCli\n"
                + "    --action <arg>    Valid actions:\n"
                + "                      <SHOW_DATABASES|CREATE_DATABASE|DROP_DATABASE|SHOW_T\n"
                + "                      ABLES|CREATE_TABLE|DROP_TABLE|SHOW_TABLE_SCHEMA|POPU\n"
                + "                      LATE_TABLE|LOAD_TABLE|SHOW_DATA>.\n"
                + "    --arg <arg>       arg for action, can be text of filepath.\n"
                + "    --catalog <arg>   catalog params json or filepath contains params\n"
                + "                      json, or you can config it through env variable\n"
                + "                      'PAIMON_CATALOG'.\n"
                + "    --help            Show help document.\n"
                + "    --verbose         Show Error Stack.\n");
    }

    @Test
    public void noCatalog() {
        PaimonCli.main(new String[] {});

        Assert.assertEquals(testOut.getContent(),
                "--catalog is mandatory if env variable 'PAIMON_CATALOG' is not set.\n"
                        + helpMessage);
    }


    @Test
    public void noAction() {
        PaimonCli.main(new String[] {"--catalog", LOCAL_CATALOG_PARAMS});

        Assert.assertEquals(testOut.getContent(),
                "Please use --action to specify action.\n" + helpMessage);
    }

    @Test
    public void wrongAction() {
        PaimonCli.main(new String[] {"--catalog", LOCAL_CATALOG_PARAMS, "--action", "wrong"});

        Assert.assertEquals(testOut.getContent(), "Wrong action type.\n" + helpMessage);
    }


    @Test
    public void dropDatabaseNotExist() {
        dropDatabase("not_exist");

        Assert.assertEquals(testOut.getContent(), "Database 'not_exist' does not exist.\n");
    }

    @Test
    public void testCreateDatabase() {
        final String dbName = "test_db";
        createDatabase(dbName);
        createDatabase(dbName);
        showDatabase();
        dropDatabase(dbName);
        showDatabase();

        Assert.assertEquals(testOut.getContent(),
                "Create database 'test_db' successfully.\n" + "Database 'test_db' already exists.\n"
                        + "Databases in catalog:\n" + "\ttest_db\n"
                        + "Drop database 'test_db' successfully.\n" + "Databases in catalog:\n");
    }

    @Test
    public void dropTableNotExist() {
        final String dbName = "test_db";
        createDatabase(dbName);
        dropTable(dbName, "not_exist");

        Assert.assertEquals(testOut.getContent(), "Create database 'test_db' successfully.\n"
                + "Table 'test_db.not_exist' does not exist.\n");
    }

    @Test
    public void testNullableTable() throws Exception {
        final String dbName = "test_db";
        final String tblName = "test_tbl";
        createDatabase(dbName);
        createTable(dbName, tblName, "/nullable_types_template.json");
        populateTable(dbName, tblName, 4096);
        showTables(dbName);
        dropTable(dbName, tblName);
        dropDatabase(dbName);

        Assert.assertEquals(testOut.getContent(), "Create database 'test_db' successfully.\n"
                + "Create table 'test_db.test_tbl' successfully.\n"
                + "Populate table 'test_db.test_tbl' successfully, 4096 rows have been generated.\n"
                + "Tables in database 'test_db':\n" + "\ttest_tbl\n"
                + "Drop table 'test_db.test_tbl' successfully.\n"
                + "Drop database 'test_db' successfully.\n");
    }

    @Test
    public void testNonNullableTable() throws Exception {
        final String dbName = "test_db";
        final String tblName = "test_tbl";
        createDatabase(dbName);
        createTable(dbName, tblName, "/non_nullable_types_template.json");
        populateTable(dbName, tblName, 4096);
        showTables(dbName);
        dropDatabase(dbName);

        Assert.assertEquals(testOut.getContent(), "Create database 'test_db' successfully.\n"
                + "Create table 'test_db.test_tbl' successfully.\n"
                + "Populate table 'test_db.test_tbl' successfully, 4096 rows have been generated.\n"
                + "Tables in database 'test_db':\n" + "\ttest_tbl\n"
                + "Dropping table 'test_db.test_tbl' ...\n"
                + "Drop database 'test_db' successfully.\n");
    }

    @Test
    public void testNullableWithPartitionTable() throws Exception {
        final String dbName = "test_db";
        final String tblName = "test_tbl";
        createDatabase(dbName);
        createTable(dbName, tblName, "/nullable_types_with_partition_template.json");
        populateTable(dbName, tblName, 32);
        showTables(dbName);
        dropDatabase(dbName);

        Assert.assertEquals(testOut.getContent(), "Create database 'test_db' successfully.\n"
                + "Create table 'test_db.test_tbl' successfully.\n"
                + "Populate table 'test_db.test_tbl' successfully, 32 rows have been generated.\n"
                + "Tables in database 'test_db':\n" + "\ttest_tbl\n"
                + "Dropping table 'test_db.test_tbl' ...\n"
                + "Drop database 'test_db' successfully.\n");
    }

    @Test
    public void testNonNullableWithPartitionTable() throws Exception {
        final String dbName = "test_db";
        final String tblName = "test_tbl";
        createDatabase(dbName);
        createTable(dbName, tblName, "/non_nullable_types_with_partition_template.json");
        populateTable(dbName, tblName, 32);
        showTables(dbName);
        dropDatabase(dbName);

        Assert.assertEquals(testOut.getContent(), "Create database 'test_db' successfully.\n"
                + "Create table 'test_db.test_tbl' successfully.\n"
                + "Populate table 'test_db.test_tbl' successfully, 32 rows have been generated.\n"
                + "Tables in database 'test_db':\n" + "\ttest_tbl\n"
                + "Dropping table 'test_db.test_tbl' ...\n"
                + "Drop database 'test_db' successfully.\n");
    }

    @Test
    public void testShowTableSchema() throws Exception {
        final String dbName = "test_db";
        createDatabase(dbName);

        createTable(dbName, "test_tbl_1", "/non_nullable_types_template.json");
        showTableSchema(dbName, "test_tbl_1");

        createTable(dbName, "test_tbl_2", "/non_nullable_types_with_partition_template.json");
        showTableSchema(dbName, "test_tbl_2");

        createTable(dbName, "test_tbl_3", "/nullable_types_template.json");
        showTableSchema(dbName, "test_tbl_3");

        createTable(dbName, "test_tbl_4", "/nullable_types_with_partition_template.json");
        showTableSchema(dbName, "test_tbl_4");

        Assert.assertEquals(testOut.getContent(), "Create database 'test_db' successfully.\n"
                + "Create table 'test_db.test_tbl_1' successfully.\n"
                + "Table 'test_db.test_tbl_1' schema:\n" + "\tFields:\n"
                + "\t\t`col_boolean` BOOLEAN NOT NULL\n" + "\t\t`col_tinyint` TINYINT NOT NULL\n"
                + "\t\t`col_smallint` SMALLINT NOT NULL\n" + "\t\t`col_int` INT NOT NULL\n"
                + "\t\t`col_bigint` BIGINT NOT NULL\n" + "\t\t`col_float` FLOAT NOT NULL\n"
                + "\t\t`col_double` DOUBLE NOT NULL\n"
                + "\t\t`col_decimal` DECIMAL(10, 2) NOT NULL\n" + "\t\t`col_date` DATE NOT NULL\n"
                + "\t\t`col_timestamp` TIMESTAMP(6) NOT NULL\n"
                + "\t\t`col_char` CHAR(10) NOT NULL\n" + "\t\t`col_varchar` VARCHAR(50) NOT NULL\n"
                + "\t\t`col_binary` BINARY(50) NOT NULL\n"
                + "\t\t`col_varbinary` VARBINARY(128) NOT NULL\n"
                + "\t\t`col_array_boolean` ARRAY<BOOLEAN NOT NULL> NOT NULL\n"
                + "\t\t`col_array_tinyint` ARRAY<TINYINT NOT NULL> NOT NULL\n"
                + "\t\t`col_array_smallint` ARRAY<SMALLINT NOT NULL> NOT NULL\n"
                + "\t\t`col_array_int` ARRAY<INT NOT NULL> NOT NULL\n"
                + "\t\t`col_array_bigint` ARRAY<BIGINT NOT NULL> NOT NULL\n"
                + "\t\t`col_array_float` ARRAY<FLOAT NOT NULL> NOT NULL\n"
                + "\t\t`col_array_double` ARRAY<DOUBLE NOT NULL> NOT NULL\n"
                + "\t\t`col_array_decimal` ARRAY<DECIMAL(32, 8) NOT NULL> NOT NULL\n"
                + "\t\t`col_array_date` ARRAY<DATE NOT NULL> NOT NULL\n"
                + "\t\t`col_array_timestamp` ARRAY<TIMESTAMP(6) NOT NULL> NOT NULL\n"
                + "\t\t`col_array_char` ARRAY<CHAR(64) NOT NULL> NOT NULL\n"
                + "\t\t`col_array_varchar` ARRAY<VARCHAR(128) NOT NULL> NOT NULL\n"
                + "\t\t`col_array_binary` ARRAY<BINARY(128) NOT NULL> NOT NULL\n"
                + "\t\t`col_array_varbinary` ARRAY<VARBINARY(256) NOT NULL> NOT NULL\n"
                + "\t\t`col_map` MAP<VARCHAR(50) NOT NULL, INT NOT NULL> NOT NULL\n"
                + "\t\t`col_struct` ROW<`field_smallint` SMALLINT NOT NULL, `field_varchar` VARCHAR(50) NOT NULL, `field_timestamp` TIMESTAMP(6) NOT NULL, `field_decimal` DECIMAL(28, 6) NOT NULL, `field_varbinary` VARBINARY(64) NOT NULL> NOT NULL\n"
                + "\t\t`col_nestedarray` ARRAY<MAP<VARCHAR(100) NOT NULL, ROW<`field_tinyint` TINYINT NOT NULL, `field_char` CHAR(50) NOT NULL> NOT NULL> NOT NULL> NOT NULL\n"
                + "\t\t`col_nestedmap` MAP<INT NOT NULL, ARRAY<MAP<VARCHAR(100) NOT NULL, ROW<`field_boolean` BOOLEAN NOT NULL, `field_timestamp` TIMESTAMP(6) NOT NULL> NOT NULL> NOT NULL> NOT NULL> NOT NULL\n"
                + "\t\t`col_nestedstruct` ROW<`field_array` ARRAY<ROW<`field_bigint` BIGINT NOT NULL> NOT NULL> NOT NULL, `field_map` MAP<CHAR(16) NOT NULL, ROW<`field_date` DATE NOT NULL> NOT NULL> NOT NULL> NOT NULL\n"
                + "\tPrimaryKeys:\n" + "\tPartitionKeys:\n" + "\tSchemaOptions:\n"
                + "\t\tpath:/tmp/paimon_ut/test_db.db/test_tbl_1\n"
                + "Create table 'test_db.test_tbl_2' successfully.\n"
                + "Table 'test_db.test_tbl_2' schema:\n" + "\tFields:\n"
                + "\t\t`col_boolean` BOOLEAN NOT NULL\n" + "\t\t`col_tinyint` TINYINT NOT NULL\n"
                + "\t\t`col_smallint` SMALLINT NOT NULL\n" + "\t\t`col_int` INT NOT NULL\n"
                + "\t\t`col_bigint` BIGINT NOT NULL\n" + "\t\t`col_float` FLOAT NOT NULL\n"
                + "\t\t`col_double` DOUBLE NOT NULL\n"
                + "\t\t`col_decimal` DECIMAL(10, 2) NOT NULL\n" + "\t\t`col_date` DATE NOT NULL\n"
                + "\t\t`col_timestamp` TIMESTAMP(6) NOT NULL\n"
                + "\t\t`col_char` CHAR(10) NOT NULL\n" + "\t\t`col_varchar` VARCHAR(50) NOT NULL\n"
                + "\t\t`col_binary` BINARY(50) NOT NULL\n"
                + "\t\t`col_varbinary` VARBINARY(128) NOT NULL\n"
                + "\t\t`col_array_boolean` ARRAY<BOOLEAN NOT NULL> NOT NULL\n"
                + "\t\t`col_array_tinyint` ARRAY<TINYINT NOT NULL> NOT NULL\n"
                + "\t\t`col_array_smallint` ARRAY<SMALLINT NOT NULL> NOT NULL\n"
                + "\t\t`col_array_int` ARRAY<INT NOT NULL> NOT NULL\n"
                + "\t\t`col_array_bigint` ARRAY<BIGINT NOT NULL> NOT NULL\n"
                + "\t\t`col_array_float` ARRAY<FLOAT NOT NULL> NOT NULL\n"
                + "\t\t`col_array_double` ARRAY<DOUBLE NOT NULL> NOT NULL\n"
                + "\t\t`col_array_decimal` ARRAY<DECIMAL(32, 8) NOT NULL> NOT NULL\n"
                + "\t\t`col_array_date` ARRAY<DATE NOT NULL> NOT NULL\n"
                + "\t\t`col_array_timestamp` ARRAY<TIMESTAMP(6) NOT NULL> NOT NULL\n"
                + "\t\t`col_array_char` ARRAY<CHAR(64) NOT NULL> NOT NULL\n"
                + "\t\t`col_array_varchar` ARRAY<VARCHAR(128) NOT NULL> NOT NULL\n"
                + "\t\t`col_array_binary` ARRAY<BINARY(128) NOT NULL> NOT NULL\n"
                + "\t\t`col_array_varbinary` ARRAY<VARBINARY(256) NOT NULL> NOT NULL\n"
                + "\t\t`col_map` MAP<VARCHAR(50) NOT NULL, INT NOT NULL> NOT NULL\n"
                + "\t\t`col_struct` ROW<`field_smallint` SMALLINT NOT NULL, `field_varchar` VARCHAR(50) NOT NULL, `field_timestamp` TIMESTAMP(6) NOT NULL, `field_decimal` DECIMAL(28, 6) NOT NULL, `field_varbinary` VARBINARY(64) NOT NULL> NOT NULL\n"
                + "\t\t`col_nestedarray` ARRAY<MAP<VARCHAR(100) NOT NULL, ROW<`field_tinyint` TINYINT NOT NULL, `field_char` CHAR(50) NOT NULL> NOT NULL> NOT NULL> NOT NULL\n"
                + "\t\t`col_nestedmap` MAP<INT NOT NULL, ARRAY<MAP<VARCHAR(100) NOT NULL, ROW<`field_boolean` BOOLEAN NOT NULL, `field_timestamp` TIMESTAMP(6) NOT NULL> NOT NULL> NOT NULL> NOT NULL> NOT NULL\n"
                + "\t\t`col_nestedstruct` ROW<`field_array` ARRAY<ROW<`field_bigint` BIGINT NOT NULL> NOT NULL> NOT NULL, `field_map` MAP<CHAR(16) NOT NULL, ROW<`field_date` DATE NOT NULL> NOT NULL> NOT NULL> NOT NULL\n"
                + "\t\t`par_int` INT NOT NULL\n" + "\t\t`par_varchar` VARCHAR(16) NOT NULL\n"
                + "\t\t`par_decimal` DECIMAL(8, 2) NOT NULL\n" + "\tPrimaryKeys:\n"
                + "\tPartitionKeys:\n" + "\t\tpar_int\n" + "\t\tpar_varchar\n" + "\t\tpar_decimal\n"
                + "\tSchemaOptions:\n" + "\t\tpath:/tmp/paimon_ut/test_db.db/test_tbl_2\n"
                + "Create table 'test_db.test_tbl_3' successfully.\n"
                + "Table 'test_db.test_tbl_3' schema:\n" + "\tFields:\n"
                + "\t\t`col_boolean` BOOLEAN\n" + "\t\t`col_tinyint` TINYINT\n"
                + "\t\t`col_smallint` SMALLINT\n" + "\t\t`col_int` INT\n"
                + "\t\t`col_bigint` BIGINT\n" + "\t\t`col_float` FLOAT\n"
                + "\t\t`col_double` DOUBLE\n" + "\t\t`col_decimal` DECIMAL(10, 2)\n"
                + "\t\t`col_date` DATE\n" + "\t\t`col_timestamp` TIMESTAMP(6)\n"
                + "\t\t`col_char` CHAR(10)\n" + "\t\t`col_varchar` VARCHAR(50)\n"
                + "\t\t`col_binary` BINARY(50)\n" + "\t\t`col_varbinary` VARBINARY(128)\n"
                + "\t\t`col_array_boolean` ARRAY<BOOLEAN>\n"
                + "\t\t`col_array_tinyint` ARRAY<TINYINT>\n"
                + "\t\t`col_array_smallint` ARRAY<SMALLINT>\n" + "\t\t`col_array_int` ARRAY<INT>\n"
                + "\t\t`col_array_bigint` ARRAY<BIGINT>\n" + "\t\t`col_array_float` ARRAY<FLOAT>\n"
                + "\t\t`col_array_double` ARRAY<DOUBLE>\n"
                + "\t\t`col_array_decimal` ARRAY<DECIMAL(32, 8)>\n"
                + "\t\t`col_array_date` ARRAY<DATE>\n"
                + "\t\t`col_array_timestamp` ARRAY<TIMESTAMP(6)>\n"
                + "\t\t`col_array_char` ARRAY<CHAR(64)>\n"
                + "\t\t`col_array_varchar` ARRAY<VARCHAR(128)>\n"
                + "\t\t`col_array_binary` ARRAY<BINARY(128)>\n"
                + "\t\t`col_array_varbinary` ARRAY<VARBINARY(256)>\n"
                + "\t\t`col_map` MAP<VARCHAR(50), INT>\n"
                + "\t\t`col_struct` ROW<`field_smallint` SMALLINT, `field_varchar` VARCHAR(50), `field_timestamp` TIMESTAMP(6), `field_decimal` DECIMAL(28, 6), `field_varbinary` VARBINARY(64)>\n"
                + "\t\t`col_nestedarray` ARRAY<MAP<VARCHAR(100), ROW<`field_tinyint` TINYINT, `field_char` CHAR(50)>>>\n"
                + "\t\t`col_nestedmap` MAP<INT, ARRAY<MAP<VARCHAR(100), ROW<`field_boolean` BOOLEAN, `field_timestamp` TIMESTAMP(6)>>>>\n"
                + "\t\t`col_nestedstruct` ROW<`field_array` ARRAY<ROW<`field_bigint` BIGINT>>, `field_map` MAP<CHAR(16) NOT NULL, ROW<`field_date` DATE>>>\n"
                + "\tPrimaryKeys:\n" + "\tPartitionKeys:\n" + "\tSchemaOptions:\n"
                + "\t\tpath:/tmp/paimon_ut/test_db.db/test_tbl_3\n"
                + "Create table 'test_db.test_tbl_4' successfully.\n"
                + "Table 'test_db.test_tbl_4' schema:\n" + "\tFields:\n"
                + "\t\t`col_boolean` BOOLEAN\n" + "\t\t`col_tinyint` TINYINT\n"
                + "\t\t`col_smallint` SMALLINT\n" + "\t\t`col_int` INT\n"
                + "\t\t`col_bigint` BIGINT\n" + "\t\t`col_float` FLOAT\n"
                + "\t\t`col_double` DOUBLE\n" + "\t\t`col_decimal` DECIMAL(10, 2)\n"
                + "\t\t`col_date` DATE\n" + "\t\t`col_timestamp` TIMESTAMP(6)\n"
                + "\t\t`col_char` CHAR(10)\n" + "\t\t`col_varchar` VARCHAR(50)\n"
                + "\t\t`col_binary` BINARY(50)\n" + "\t\t`col_varbinary` VARBINARY(128)\n"
                + "\t\t`col_array_boolean` ARRAY<BOOLEAN>\n"
                + "\t\t`col_array_tinyint` ARRAY<TINYINT>\n"
                + "\t\t`col_array_smallint` ARRAY<SMALLINT>\n" + "\t\t`col_array_int` ARRAY<INT>\n"
                + "\t\t`col_array_bigint` ARRAY<BIGINT>\n" + "\t\t`col_array_float` ARRAY<FLOAT>\n"
                + "\t\t`col_array_double` ARRAY<DOUBLE>\n"
                + "\t\t`col_array_decimal` ARRAY<DECIMAL(32, 8)>\n"
                + "\t\t`col_array_date` ARRAY<DATE>\n"
                + "\t\t`col_array_timestamp` ARRAY<TIMESTAMP(6)>\n"
                + "\t\t`col_array_char` ARRAY<CHAR(64)>\n"
                + "\t\t`col_array_varchar` ARRAY<VARCHAR(128)>\n"
                + "\t\t`col_array_binary` ARRAY<BINARY(128)>\n"
                + "\t\t`col_array_varbinary` ARRAY<VARBINARY(256)>\n"
                + "\t\t`col_map` MAP<VARCHAR(50), INT>\n"
                + "\t\t`col_struct` ROW<`field_smallint` SMALLINT, `field_varchar` VARCHAR(50), `field_timestamp` TIMESTAMP(6), `field_decimal` DECIMAL(28, 6), `field_varbinary` VARBINARY(64)>\n"
                + "\t\t`col_nestedarray` ARRAY<MAP<VARCHAR(100), ROW<`field_tinyint` TINYINT, `field_char` CHAR(50)>>>\n"
                + "\t\t`col_nestedmap` MAP<INT, ARRAY<MAP<VARCHAR(100), ROW<`field_boolean` BOOLEAN, `field_timestamp` TIMESTAMP(6)>>>>\n"
                + "\t\t`col_nestedstruct` ROW<`field_array` ARRAY<ROW<`field_bigint` BIGINT>>, `field_map` MAP<CHAR(16) NOT NULL, ROW<`field_date` DATE>>>\n"
                + "\t\t`par_int` INT\n" + "\t\t`par_varchar` VARCHAR(16)\n"
                + "\t\t`par_decimal` DECIMAL(8, 2)\n" + "\tPrimaryKeys:\n" + "\tPartitionKeys:\n"
                + "\t\tpar_int\n" + "\t\tpar_varchar\n" + "\t\tpar_decimal\n" + "\tSchemaOptions:\n"
                + "\t\tpath:/tmp/paimon_ut/test_db.db/test_tbl_4\n");
    }

    @Test
    public void testCENullableTable() throws Exception {
        final String dbName = "test_db";
        final String tblName = "test_tbl";
        createDatabase(dbName);
        createTable(dbName, tblName, "/ce_nullable_types_template.json");
        populateTable(dbName, tblName, 4096);
        showTables(dbName);
        dropTable(dbName, tblName);
        dropDatabase(dbName);

        Assert.assertEquals(testOut.getContent(), "Create database 'test_db' successfully.\n"
                + "Create table 'test_db.test_tbl' successfully.\n"
                + "Populate table 'test_db.test_tbl' successfully, 4096 rows have been generated.\n"
                + "Tables in database 'test_db':\n" + "\ttest_tbl\n"
                + "Drop table 'test_db.test_tbl' successfully.\n"
                + "Drop database 'test_db' successfully.\n");
    }

    @Test
    public void testCENonNullableTable() throws Exception {
        final String dbName = "test_db";
        final String tblName = "test_tbl";
        createDatabase(dbName);
        createTable(dbName, tblName, "/ce_non_nullable_types_template.json");
        populateTable(dbName, tblName, 4096);
        showTables(dbName);
        dropDatabase(dbName);

        Assert.assertEquals(testOut.getContent(), "Create database 'test_db' successfully.\n"
                + "Create table 'test_db.test_tbl' successfully.\n"
                + "Populate table 'test_db.test_tbl' successfully, 4096 rows have been generated.\n"
                + "Tables in database 'test_db':\n" + "\ttest_tbl\n"
                + "Dropping table 'test_db.test_tbl' ...\n"
                + "Drop database 'test_db' successfully.\n");
    }

    @Test
    public void testCENullableWithPartitionTable() throws Exception {
        final String dbName = "test_db";
        final String tblName = "test_tbl";
        createDatabase(dbName);
        createTable(dbName, tblName, "/ce_nullable_types_with_partition_template.json");
        populateTable(dbName, tblName, 32);
        showTables(dbName);
        dropDatabase(dbName);

        Assert.assertEquals(testOut.getContent(), "Create database 'test_db' successfully.\n"
                + "Create table 'test_db.test_tbl' successfully.\n"
                + "Populate table 'test_db.test_tbl' successfully, 32 rows have been generated.\n"
                + "Tables in database 'test_db':\n" + "\ttest_tbl\n"
                + "Dropping table 'test_db.test_tbl' ...\n"
                + "Drop database 'test_db' successfully.\n");
    }

    @Test
    public void testCENonNullableWithPartitionTable() throws Exception {
        final String dbName = "test_db";
        final String tblName = "test_tbl";
        createDatabase(dbName);
        createTable(dbName, tblName, "/ce_non_nullable_types_with_partition_template.json");
        populateTable(dbName, tblName, 32);
        showTables(dbName);
        dropDatabase(dbName);

        Assert.assertEquals(testOut.getContent(), "Create database 'test_db' successfully.\n"
                + "Create table 'test_db.test_tbl' successfully.\n"
                + "Populate table 'test_db.test_tbl' successfully, 32 rows have been generated.\n"
                + "Tables in database 'test_db':\n" + "\ttest_tbl\n"
                + "Dropping table 'test_db.test_tbl' ...\n"
                + "Drop database 'test_db' successfully.\n");
    }

    @Test
    public void testPartitionPredicate() throws Exception {
        final String dbName = "test_partition_db";
        createDatabase(dbName);

        createTable(dbName, "bool_partition_tbl", "/partition/ce_bool_partition_template.json");
        populateTable(dbName, "bool_partition_tbl", 16);

        createTable(dbName, "char_partition_tbl", "/partition/ce_char_partition_template.json");
        populateTable(dbName, "char_partition_tbl", 16);

        createTable(dbName, "decimal_partition_tbl",
                "/partition/ce_decimal_partition_template.json");
        populateTable(dbName, "decimal_partition_tbl", 16);

        createTable(dbName, "float_partition_tbl", "/partition/ce_float_partition_template.json");
        populateTable(dbName, "float_partition_tbl", 16);

        createTable(dbName, "integer_partition_tbl",
                "/partition/ce_integer_partition_template.json");
        populateTable(dbName, "integer_partition_tbl", 16);

        createTable(dbName, "time_partition_tbl", "/partition/ce_time_partition_template.json");
        populateTable(dbName, "time_partition_tbl", 16);

        showTables(dbName);
        dropDatabase(dbName);

        Assert.assertEquals(testOut.getContent(),
                "Create database 'test_partition_db' successfully.\n"
                        + "Create table 'test_partition_db.bool_partition_tbl' successfully.\n"
                        + "Populate table 'test_partition_db.bool_partition_tbl' successfully, 16 rows have been generated.\n"
                        + "Create table 'test_partition_db.char_partition_tbl' successfully.\n"
                        + "Populate table 'test_partition_db.char_partition_tbl' successfully, 16 rows have been generated.\n"
                        + "Create table 'test_partition_db.decimal_partition_tbl' successfully.\n"
                        + "Populate table 'test_partition_db.decimal_partition_tbl' successfully, 16 rows have been generated.\n"
                        + "Create table 'test_partition_db.float_partition_tbl' successfully.\n"
                        + "Populate table 'test_partition_db.float_partition_tbl' successfully, 16 rows have been generated.\n"
                        + "Create table 'test_partition_db.integer_partition_tbl' successfully.\n"
                        + "Populate table 'test_partition_db.integer_partition_tbl' successfully, 16 rows have been generated.\n"
                        + "Create table 'test_partition_db.time_partition_tbl' successfully.\n"
                        + "Populate table 'test_partition_db.time_partition_tbl' successfully, 16 rows have been generated.\n"
                        + "Tables in database 'test_partition_db':\n" + "\tbool_partition_tbl\n"
                        + "\tchar_partition_tbl\n" + "\tdecimal_partition_tbl\n"
                        + "\tfloat_partition_tbl\n" + "\tinteger_partition_tbl\n"
                        + "\ttime_partition_tbl\n"
                        + "Dropping table 'test_partition_db.bool_partition_tbl' ...\n"
                        + "Dropping table 'test_partition_db.char_partition_tbl' ...\n"
                        + "Dropping table 'test_partition_db.decimal_partition_tbl' ...\n"
                        + "Dropping table 'test_partition_db.float_partition_tbl' ...\n"
                        + "Dropping table 'test_partition_db.integer_partition_tbl' ...\n"
                        + "Dropping table 'test_partition_db.time_partition_tbl' ...\n"
                        + "Drop database 'test_partition_db' successfully.\n");
    }

    @Test
    public void testLoadNonNullableTableBatchSize1() throws Exception {
        final String dbName = "test_load_db";
        final String tblName = "non_nullable_table";
        createDatabase(dbName);
        createTable(dbName, tblName, "/load/non_nullable_load_types_template.json");
        loadTable(dbName, tblName, "/load/non_nullable_load_types.csv", 1, ',');

        Assert.assertEquals(testOut.getContent(), "Create database 'test_load_db' successfully.\n"
                + "Create table 'test_load_db.non_nullable_table' successfully.\n"
                + "Load table 'test_load_db.non_nullable_table' successfully, 5 rows have been inserted.\n");
    }

    @Test
    public void testLoadNonNullableTableBatchSize4096() throws Exception {
        final String dbName = "test_load_db";
        final String tblName = "non_nullable_table";
        createDatabase(dbName);
        createTable(dbName, tblName, "/load/non_nullable_load_types_template.json");
        loadTable(dbName, tblName, "/load/non_nullable_load_types.csv", 4096, ',');

        Assert.assertEquals(testOut.getContent(), "Create database 'test_load_db' successfully.\n"
                + "Create table 'test_load_db.non_nullable_table' successfully.\n"
                + "Load table 'test_load_db.non_nullable_table' successfully, 5 rows have been inserted.\n");
    }

    @Test
    public void testLoadNullableTableBatchSize1() throws Exception {
        final String dbName = "test_load_db_2";
        final String tblName = "nullable_table";
        createDatabase(dbName);
        createTable(dbName, tblName, "/load/nullable_load_types_template.json");
        loadTable(dbName, tblName, "/load/nullable_load_types.csv", 1, ',');

        Assert.assertEquals(testOut.getContent(), "Create database 'test_load_db_2' successfully.\n"
                + "Create table 'test_load_db_2.nullable_table' successfully.\n"
                + "Load table 'test_load_db_2.nullable_table' successfully, 17 rows have been inserted.\n");
    }

    @Test
    public void testLoadNullableTableBatchSize4096() throws Exception {
        final String dbName = "test_load_db_2";
        final String tblName = "nullable_table";
        createDatabase(dbName);
        createTable(dbName, tblName, "/load/nullable_load_types_template.json");
        loadTable(dbName, tblName, "/load/nullable_load_types.csv", 4096, ',');

        Assert.assertEquals(testOut.getContent(), "Create database 'test_load_db_2' successfully.\n"
                + "Create table 'test_load_db_2.nullable_table' successfully.\n"
                + "Load table 'test_load_db_2.nullable_table' successfully, 17 rows have been inserted.\n");
    }

    @Test
    public void testShowDataWithAllFields() throws Exception {
        final String dbName = "test_show_data";
        final String tblName = "test_show_table_with_all_type";
        createDatabase(dbName);
        createTable(dbName, tblName, "/load/nullable_load_types_template.json");
        loadTable(dbName, tblName, "/load/nullable_load_types.csv", 4096, ',');
        showData(dbName, tblName);

        List<String> lines = Lists.newArrayList(testOut.getContent().split("\\n")).stream()
                .map(line -> line.replaceFirst("[0-9]+: ", "")).sorted()
                .collect(Collectors.toList());
        String sortedOutput = String.join("\n", lines);
        Assert.assertEquals(sortedOutput, "Create database 'test_show_data' successfully.\n"
                + "Create table 'test_show_data.test_show_table_with_all_type' successfully.\n"
                + "Load table 'test_show_data.test_show_table_with_all_type' successfully, 17 rows have been inserted.\n"
                + "NULL, 1, 1, 1, 1, 1.1, 1.1, 11.11, 2023-06-01, 2023-06-01 10:10:10.111111, \"1\", \"1\", \"1\", \"1\"\n"
                + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL\n"
                + "col_boolean, col_tinyint, col_smallint, col_int, col_bigint, col_float, col_double, col_decimal, col_date, col_timestamp, col_char, col_varchar, col_binary, col_varbinary\n"
                + "false, 2, 2, 2, 2, 2.2, 2.2, 12.12, 2023-06-02, 2023-06-02 10:10:10.111111, NULL, \"2\", \"2\", \"2\"\n"
                + "false, 2, 2, 2, 2, 2.2, NULL, 12.12, 2023-06-02, 2023-06-02 10:10:10.111111, \"2\", \"2\", \"2\", \"2\"\n"
                + "false, 3, 3, 3, 3, 3.3, 3.3, 13.13, 2023-06-03, 2023-06-03 10:10:10.111111, \"3\", NULL, \"3\", \"3\"\n"
                + "false, 3, 3, 3, 3, 3.3, 3.3, NULL, 2023-06-03, 2023-06-03 10:10:10.111111, \"3\", \"3\", \"3\", \"3\"\n"
                + "false, 3, NULL, 3, 3, 3.3, 3.3, 13.13, 2023-06-03, 2023-06-03 10:10:10.111111, \"3\", \"3\", \"3\", \"3\"\n"
                + "false, NULL, 2, 2, 2, 2.2, 2.2, 12.12, 2023-06-02, 2023-06-02 10:10:10.111111, \"2\", \"2\", \"2\", \"2\"\n"
                + "true, 1, 1, 1, 1, 1.1, 1.1, 11.11, 2023-06-01, 2023-06-01 10:10:10.111111, \"1\", \"1\", \"1\", \"1\"\n"
                + "true, 1, 1, 1, 1, 1.1, 1.1, 11.11, 2023-06-01, NULL, \"1\", \"1\", \"1\", \"1\"\n"
                + "true, 1, 1, 1, 1, NULL, 1.1, 11.11, 2023-06-01, 2023-06-01 10:10:10.111111, \"1\", \"1\", \"1\", \"1\"\n"
                + "true, 4, 4, 4, 4, 4.4, 4.4, 14.14, 2023-06-04, 2023-06-04 10:10:10.111111, \"4\", \"4\", NULL, \"4\"\n"
                + "true, 4, 4, 4, 4, 4.4, 4.4, 14.14, NULL, 2023-06-04 10:10:10.111111, \"4\", \"4\", \"4\", \"4\"\n"
                + "true, 4, 4, NULL, 4, 4.4, 4.4, 14.14, 2023-06-04, 2023-06-04 10:10:10.111111, \"4\", \"4\", \"4\", \"4\"\n"
                + "true, 5, 5, 5, 5, 5.5, 5.5, 15.15, 2023-06-05, 2023-06-05 10:10:10.111111, \"5\", \"5\", \"5\", \"5\"\n"
                + "true, 5, 5, 5, 5, 5.5, 5.5, 15.15, 2023-06-05, 2023-06-05 10:10:10.111111, \"5\", \"5\", \"5\", NULL\n"
                + "true, 5, 5, 5, NULL, 5.5, 5.5, 15.15, 2023-06-05, 2023-06-05 10:10:10.111111, \"5\", \"5\", \"5\", \"5\"");
    }

    @Test
    public void testShowDataWithSelectedFields1() throws Exception {
        final String dbName = "test_show_data_with_selected_fields_1";
        final String tblName = "test_show_table_with_all_type";
        createDatabase(dbName);
        createTable(dbName, tblName, "/load/nullable_load_types_template.json");
        loadTable(dbName, tblName, "/load/nullable_load_types.csv", 4096, ',');
        showData(dbName, tblName, "col_timestamp");

        List<String> lines = Lists.newArrayList(testOut.getContent().split("\\n")).stream()
                .map(line -> line.replaceFirst("[0-9]+: ", "")).sorted()
                .collect(Collectors.toList());
        String sortedOutput = String.join("\n", lines);
        Assert.assertEquals(sortedOutput, "2023-06-01 10:10:10.111111\n"
                + "2023-06-01 10:10:10.111111\n" + "2023-06-01 10:10:10.111111\n"
                + "2023-06-02 10:10:10.111111\n" + "2023-06-02 10:10:10.111111\n"
                + "2023-06-02 10:10:10.111111\n" + "2023-06-03 10:10:10.111111\n"
                + "2023-06-03 10:10:10.111111\n" + "2023-06-03 10:10:10.111111\n"
                + "2023-06-04 10:10:10.111111\n" + "2023-06-04 10:10:10.111111\n"
                + "2023-06-04 10:10:10.111111\n" + "2023-06-05 10:10:10.111111\n"
                + "2023-06-05 10:10:10.111111\n" + "2023-06-05 10:10:10.111111\n"
                + "Create database 'test_show_data_with_selected_fields_1' successfully.\n"
                + "Create table 'test_show_data_with_selected_fields_1.test_show_table_with_all_type' successfully.\n"
                + "Load table 'test_show_data_with_selected_fields_1.test_show_table_with_all_type' successfully, 17 rows have been inserted.\n"
                + "NULL\n" + "NULL\n" + "col_timestamp");
    }

    @Test
    public void testShowDataWithSelectedFields2() throws Exception {
        final String dbName = "test_show_data_with_selected_fields_2";
        final String tblName = "test_show_table_with_all_type";
        createDatabase(dbName);
        createTable(dbName, tblName, "/load/nullable_load_types_template.json");
        loadTable(dbName, tblName, "/load/nullable_load_types.csv", 4096, ',');
        showData(dbName, tblName, "col_int", "col_timestamp", "col_decimal");

        List<String> lines = Lists.newArrayList(testOut.getContent().split("\\n")).stream()
                .map(line -> line.replaceFirst("[0-9]+: ", "")).sorted()
                .collect(Collectors.toList());
        String sortedOutput = String.join("\n", lines);
        Assert.assertEquals(sortedOutput, "1, 11.11, 2023-06-01 10:10:10.111111\n"
                + "1, 11.11, 2023-06-01 10:10:10.111111\n"
                + "1, 11.11, 2023-06-01 10:10:10.111111\n" + "1, 11.11, NULL\n"
                + "2, 12.12, 2023-06-02 10:10:10.111111\n"
                + "2, 12.12, 2023-06-02 10:10:10.111111\n"
                + "2, 12.12, 2023-06-02 10:10:10.111111\n"
                + "3, 13.13, 2023-06-03 10:10:10.111111\n"
                + "3, 13.13, 2023-06-03 10:10:10.111111\n" + "3, NULL, 2023-06-03 10:10:10.111111\n"
                + "4, 14.14, 2023-06-04 10:10:10.111111\n"
                + "4, 14.14, 2023-06-04 10:10:10.111111\n"
                + "5, 15.15, 2023-06-05 10:10:10.111111\n"
                + "5, 15.15, 2023-06-05 10:10:10.111111\n"
                + "5, 15.15, 2023-06-05 10:10:10.111111\n"
                + "Create database 'test_show_data_with_selected_fields_2' successfully.\n"
                + "Create table 'test_show_data_with_selected_fields_2.test_show_table_with_all_type' successfully.\n"
                + "Load table 'test_show_data_with_selected_fields_2.test_show_table_with_all_type' successfully, 17 rows have been inserted.\n"
                + "NULL, 14.14, 2023-06-04 10:10:10.111111\n" + "NULL, NULL, NULL\n"
                + "col_int, col_decimal, col_timestamp");
    }
}
