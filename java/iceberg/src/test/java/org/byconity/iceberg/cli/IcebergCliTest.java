package org.byconity.iceberg.cli;

import com.google.common.collect.Lists;
import org.apache.iceberg.FileContent;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class IcebergCliTest extends IcebergCliTestBase {

    @Test
    public void help() {
        Assert.assertEquals(helpMessage, "usage: IcebergCli\n"
                + "    --action <arg>    Valid actions:\n"
                + "                      <SHOW_DATABASES|CREATE_DATABASE|DROP_DATABASE|SHOW_T\n"
                + "                      ABLES|CREATE_TABLE|DROP_TABLE|SHOW_TABLE_SCHEMA|POPU\n"
                + "                      LATE_TABLE|LOAD_TABLE|SHOW_DATA|DELETE_TABLE>.\n"
                + "    --arg <arg>       arg for action, can be text of filepath.\n"
                + "    --catalog <arg>   catalog params json or filepath contains params\n"
                + "                      json, or you can config it through env variable\n"
                + "                      'ICEBERG_CATALOG'.\n"
                + "    --help            Show help document.\n"
                + "    --verbose         Show Error Stack.\n", helpMessage);
    }

    @Test
    public void noCatalog() {
        IcebergCli.main(new String[] {});

        Assert.assertEquals(testOut.getContent(),
                "--catalog is mandatory if env variable 'ICEBERG_CATALOG' is not set.\n"
                        + helpMessage,
                testOut.getContent());
    }

    @Test
    public void noAction() {
        IcebergCli.main(new String[] {"--catalog", LOCAL_CATALOG_PARAMS});

        Assert.assertEquals(testOut.getContent(),
                "Please use --action to specify action.\n" + helpMessage, testOut.getContent());
    }

    @Test
    public void wrongAction() {
        IcebergCli.main(new String[] {"--catalog", LOCAL_CATALOG_PARAMS, "--action", "wrong"});

        Assert.assertEquals(testOut.getContent(), "Wrong action type.\n" + helpMessage,
                testOut.getContent());
    }

    @Test
    public void dropDatabaseNotExist() {
        dropDatabase("not_exist");

        Assert.assertEquals(testOut.getContent(), "Database 'not_exist' does not exist.\n",
                testOut.getContent());
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
                        + "Drop database 'test_db' successfully.\n" + "Databases in catalog:\n",
                testOut.getContent());
    }

    @Test
    public void dropTableNotExist() {
        final String dbName = "test_db";
        createDatabase(dbName);
        dropTable(dbName, "not_exist");

        Assert.assertEquals(testOut.getContent(), "Create database 'test_db' successfully.\n"
                + "Table 'test_db.not_exist' does not exist.\n", testOut.getContent());
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
                + "Drop database 'test_db' successfully.\n", testOut.getContent());
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
                + "Drop database 'test_db' successfully.\n", testOut.getContent());
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
                + "Drop database 'test_db' successfully.\n", testOut.getContent());
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
                + "Drop database 'test_db' successfully.\n", testOut.getContent());
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
                + "\t\t1: col_boolean: required boolean\n" + "\t\t2: col_integer: required int\n"
                + "\t\t3: col_long: required long\n" + "\t\t4: col_float: required float\n"
                + "\t\t5: col_double: required double\n"
                + "\t\t6: col_decimal: required decimal(10, 2)\n"
                + "\t\t7: col_date: required date\n" + "\t\t8: col_timestamp: required timestamp\n"
                + "\t\t9: col_uuid: required uuid\n" + "\t\t10: col_string: required string\n"
                + "\t\t11: col_fixed: required fixed[50]\n"
                + "\t\t12: col_binary: required binary\n"
                + "\t\t13: col_list_boolean: required list<boolean>\n"
                + "\t\t14: col_list_integer: required list<int>\n"
                + "\t\t15: col_list_long: required list<long>\n"
                + "\t\t16: col_list_float: required list<float>\n"
                + "\t\t17: col_list_double: required list<double>\n"
                + "\t\t18: col_list_decimal: required list<decimal(32, 8)>\n"
                + "\t\t19: col_list_date: required list<date>\n"
                + "\t\t20: col_list_timestamp: required list<timestamp>\n"
                + "\t\t21: col_list_uuid: required list<uuid>\n"
                + "\t\t22: col_list_string: required list<string>\n"
                + "\t\t23: col_list_fixed: required list<fixed[128]>\n"
                + "\t\t24: col_list_binary: required list<binary>\n"
                + "\t\t25: col_map: required map<string, int>\n"
                + "\t\t26: col_struct: required struct<44: field_integer: required int, 45: field_string: required string, 46: field_timestamp: required timestamp, 47: field_decimal: required decimal(28, 6), 48: field_binary: required binary>\n"
                + "\t\t27: col_nestedlist: required list<map<string, struct<52: field_long: required long, 53: field_string: required string>>>\n"
                + "\t\t28: col_nestedmap: required map<int, list<map<string, struct<59: field_boolean: required boolean, 60: field_timestamp: required timestamp>>>>\n"
                + "\t\t29: col_nestedstruct: required struct<61: field_list: required list<struct<64: field_long: required long>>, 62: field_map: required map<string, struct<67: field_date: required date>>>\n"
                + "\tPartitionFields:\n" + "\tSchemaProperties:\n"
                + "\t\twrite.parquet.compression-codec:zstd\n"
                + "Create table 'test_db.test_tbl_2' successfully.\n"
                + "Table 'test_db.test_tbl_2' schema:\n" + "\tFields:\n"
                + "\t\t1: col_boolean: required boolean\n" + "\t\t2: col_integer: required int\n"
                + "\t\t3: col_long: required long\n" + "\t\t4: col_float: required float\n"
                + "\t\t5: col_double: required double\n"
                + "\t\t6: col_decimal: required decimal(10, 2)\n"
                + "\t\t7: col_date: required date\n" + "\t\t8: col_timestamp: required timestamp\n"
                + "\t\t9: col_uuid: required uuid\n" + "\t\t10: col_string: required string\n"
                + "\t\t11: col_fixed: required fixed[50]\n"
                + "\t\t12: col_binary: required binary\n"
                + "\t\t13: col_list_boolean: required list<boolean>\n"
                + "\t\t14: col_list_integer: required list<int>\n"
                + "\t\t15: col_list_long: required list<long>\n"
                + "\t\t16: col_list_float: required list<float>\n"
                + "\t\t17: col_list_double: required list<double>\n"
                + "\t\t18: col_list_decimal: required list<decimal(32, 8)>\n"
                + "\t\t19: col_list_date: required list<date>\n"
                + "\t\t20: col_list_timestamp: required list<timestamp>\n"
                + "\t\t21: col_list_uuid: required list<uuid>\n"
                + "\t\t22: col_list_string: required list<string>\n"
                + "\t\t23: col_list_fixed: required list<fixed[128]>\n"
                + "\t\t24: col_list_binary: required list<binary>\n"
                + "\t\t25: col_map: required map<string, int>\n"
                + "\t\t26: col_struct: required struct<47: field_integer: required int, 48: field_string: required string, 49: field_timestamp: required timestamp, 50: field_decimal: required decimal(28, 6), 51: field_binary: required binary>\n"
                + "\t\t27: col_nestedarray: required list<map<string, struct<55: field_long: required long, 56: field_string: required string>>>\n"
                + "\t\t28: col_nestedmap: required map<int, list<map<string, struct<62: field_boolean: required boolean, 63: field_timestamp: required timestamp>>>>\n"
                + "\t\t29: col_nestedstruct: required struct<64: field_array: required list<struct<67: field_long: required long>>, 65: field_map: required map<string, struct<70: field_date: required date>>>\n"
                + "\t\t30: par_integer: required int\n" + "\t\t31: par_string: required string\n"
                + "\t\t32: par_decimal: required decimal(8, 2)\n" + "\tPartitionFields:\n"
                + "\t\t1000: par_integer: identity(30)\n" + "\t\t1001: par_string: identity(31)\n"
                + "\t\t1002: par_decimal: identity(32)\n" + "\tSchemaProperties:\n"
                + "\t\twrite.parquet.compression-codec:zstd\n"
                + "Create table 'test_db.test_tbl_3' successfully.\n"
                + "Table 'test_db.test_tbl_3' schema:\n" + "\tFields:\n"
                + "\t\t1: col_boolean: optional boolean\n" + "\t\t2: col_integer: optional int\n"
                + "\t\t3: col_long: optional long\n" + "\t\t4: col_float: optional float\n"
                + "\t\t5: col_double: optional double\n"
                + "\t\t6: col_decimal: optional decimal(10, 2)\n"
                + "\t\t7: col_date: optional date\n" + "\t\t8: col_timestamp: optional timestamp\n"
                + "\t\t9: col_uuid: optional uuid\n" + "\t\t10: col_string: optional string\n"
                + "\t\t11: col_fixed: optional fixed[50]\n"
                + "\t\t12: col_binary: optional binary\n"
                + "\t\t13: col_list_boolean: optional list<boolean>\n"
                + "\t\t14: col_list_integer: optional list<int>\n"
                + "\t\t15: col_list_long: optional list<long>\n"
                + "\t\t16: col_list_float: optional list<float>\n"
                + "\t\t17: col_list_double: optional list<double>\n"
                + "\t\t18: col_list_decimal: optional list<decimal(32, 8)>\n"
                + "\t\t19: col_list_date: optional list<date>\n"
                + "\t\t20: col_list_timestamp: optional list<timestamp>\n"
                + "\t\t21: col_list_uuid: optional list<uuid>\n"
                + "\t\t22: col_list_string: optional list<string>\n"
                + "\t\t23: col_list_fixed: optional list<fixed[128]>\n"
                + "\t\t24: col_list_binary: optional list<binary>\n"
                + "\t\t25: col_map: optional map<string, int>\n"
                + "\t\t26: col_struct: optional struct<44: field_integer: optional int, 45: field_string: optional string, 46: field_timestamp: optional timestamp, 47: field_decimal: optional decimal(28, 6), 48: field_binary: optional binary>\n"
                + "\t\t27: col_nestedlist: optional list<map<string, struct<52: field_long: optional long, 53: field_string: optional string>>>\n"
                + "\t\t28: col_nestedmap: optional map<int, list<map<string, struct<59: field_boolean: optional boolean, 60: field_timestamp: optional timestamp>>>>\n"
                + "\t\t29: col_nestedstruct: optional struct<61: field_list: optional list<struct<64: field_long: optional long>>, 62: field_map: optional map<string, struct<67: field_date: optional date>>>\n"
                + "\tPartitionFields:\n" + "\tSchemaProperties:\n"
                + "\t\twrite.parquet.compression-codec:zstd\n"
                + "Create table 'test_db.test_tbl_4' successfully.\n"
                + "Table 'test_db.test_tbl_4' schema:\n" + "\tFields:\n"
                + "\t\t1: col_boolean: optional boolean\n" + "\t\t2: col_integer: optional int\n"
                + "\t\t3: col_long: optional long\n" + "\t\t4: col_float: optional float\n"
                + "\t\t5: col_double: optional double\n"
                + "\t\t6: col_decimal: optional decimal(10, 2)\n"
                + "\t\t7: col_date: optional date\n" + "\t\t8: col_timestamp: optional timestamp\n"
                + "\t\t9: col_uuid: optional uuid\n" + "\t\t10: col_string: optional string\n"
                + "\t\t11: col_fixed: optional fixed[50]\n"
                + "\t\t12: col_binary: optional binary\n"
                + "\t\t13: col_list_boolean: optional list<boolean>\n"
                + "\t\t14: col_list_integer: optional list<int>\n"
                + "\t\t15: col_list_long: optional list<long>\n"
                + "\t\t16: col_list_float: optional list<float>\n"
                + "\t\t17: col_list_double: optional list<double>\n"
                + "\t\t18: col_list_decimal: optional list<decimal(32, 8)>\n"
                + "\t\t19: col_list_date: optional list<date>\n"
                + "\t\t20: col_list_timestamp: optional list<timestamp>\n"
                + "\t\t21: col_list_uuid: optional list<uuid>\n"
                + "\t\t22: col_list_string: optional list<string>\n"
                + "\t\t23: col_list_fixed: optional list<fixed[128]>\n"
                + "\t\t24: col_list_binary: optional list<binary>\n"
                + "\t\t25: col_map: optional map<string, int>\n"
                + "\t\t26: col_struct: optional struct<47: field_integer: optional int, 48: field_string: optional string, 49: field_timestamp: optional timestamp, 50: field_decimal: optional decimal(28, 6), 51: field_binary: optional binary>\n"
                + "\t\t27: col_nestedarray: optional list<map<string, struct<55: field_long: optional long, 56: field_string: optional string>>>\n"
                + "\t\t28: col_nestedmap: optional map<int, list<map<string, struct<62: field_boolean: optional boolean, 63: field_timestamp: optional timestamp>>>>\n"
                + "\t\t29: col_nestedstruct: optional struct<64: field_array: optional list<struct<67: field_long: optional long>>, 65: field_map: optional map<string, struct<70: field_date: optional date>>>\n"
                + "\t\t30: par_integer: optional int\n" + "\t\t31: par_string: optional string\n"
                + "\t\t32: par_decimal: optional decimal(8, 2)\n" + "\tPartitionFields:\n"
                + "\t\t1000: par_integer: identity(30)\n" + "\t\t1001: par_string: identity(31)\n"
                + "\t\t1002: par_decimal: identity(32)\n" + "\tSchemaProperties:\n"
                + "\t\twrite.parquet.compression-codec:zstd\n", testOut.getContent());
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
                + "Drop database 'test_db' successfully.\n", testOut.getContent());
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
                + "Drop database 'test_db' successfully.\n", testOut.getContent());
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
                + "Drop database 'test_db' successfully.\n", testOut.getContent());
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
                + "Drop database 'test_db' successfully.\n", testOut.getContent());
    }

    @Test
    public void testPartitionPredicate() throws Exception {
        final String dbName = "test_partition_db";
        createDatabase(dbName);

        createTable(dbName, "bool_partition_tbl", "/partition/ce_bool_partition_template.json");
        populateTable(dbName, "bool_partition_tbl", 16);

        createTable(dbName, "char_partition_tbl", "/partition/ce_string_partition_template.json");
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
                        + "Tables in database 'test_partition_db':\n" + "\tchar_partition_tbl\n"
                        + "\ttime_partition_tbl\n" + "\tinteger_partition_tbl\n"
                        + "\tfloat_partition_tbl\n" + "\tdecimal_partition_tbl\n"
                        + "\tbool_partition_tbl\n"
                        + "Dropping table 'test_partition_db.char_partition_tbl' ...\n"
                        + "Dropping table 'test_partition_db.time_partition_tbl' ...\n"
                        + "Dropping table 'test_partition_db.integer_partition_tbl' ...\n"
                        + "Dropping table 'test_partition_db.float_partition_tbl' ...\n"
                        + "Dropping table 'test_partition_db.decimal_partition_tbl' ...\n"
                        + "Dropping table 'test_partition_db.bool_partition_tbl' ...\n"
                        + "Drop database 'test_partition_db' successfully.\n",
                testOut.getContent());
    }

    @Test
    public void testLoadNonNullableTableMaxRowsPerFile1() throws Exception {
        final String dbName = "test_load_db";
        final String tblName = "non_nullable_table";
        createDatabase(dbName);
        createTable(dbName, tblName, "/load/non_nullable_load_types_template.json");
        loadTable(dbName, tblName, "/load/non_nullable_load_types.csv", 1, ',');

        Assert.assertEquals(testOut.getContent(), "Create database 'test_load_db' successfully.\n"
                + "Create table 'test_load_db.non_nullable_table' successfully.\n"
                + "Load table 'test_load_db.non_nullable_table' successfully, 5 rows have been inserted.\n",
                testOut.getContent());
    }

    @Test
    public void testLoadNonNullableTableMaxRowsPerFile4096() throws Exception {
        final String dbName = "test_load_db";
        final String tblName = "non_nullable_table";
        createDatabase(dbName);
        createTable(dbName, tblName, "/load/non_nullable_load_types_template.json");
        loadTable(dbName, tblName, "/load/non_nullable_load_types.csv", 4096, ',');

        Assert.assertEquals(testOut.getContent(), "Create database 'test_load_db' successfully.\n"
                + "Create table 'test_load_db.non_nullable_table' successfully.\n"
                + "Load table 'test_load_db.non_nullable_table' successfully, 5 rows have been inserted.\n",
                testOut.getContent());
    }

    @Test
    public void testLoadNullableTableMaxRowsPerFile1() throws Exception {
        final String dbName = "test_load_db_2";
        final String tblName = "nullable_table";
        createDatabase(dbName);
        createTable(dbName, tblName, "/load/nullable_load_types_template.json");
        loadTable(dbName, tblName, "/load/nullable_load_types.csv", 1, ',');

        Assert.assertEquals(testOut.getContent(), "Create database 'test_load_db_2' successfully.\n"
                + "Create table 'test_load_db_2.nullable_table' successfully.\n"
                + "Load table 'test_load_db_2.nullable_table' successfully, 17 rows have been inserted.\n",
                testOut.getContent());
    }

    @Test
    public void testLoadNullableTableMaxRowsPerFile4096() throws Exception {
        final String dbName = "test_load_db_2";
        final String tblName = "nullable_table";
        createDatabase(dbName);
        createTable(dbName, tblName, "/load/nullable_load_types_template.json");
        loadTable(dbName, tblName, "/load/nullable_load_types.csv", 4096, ',');

        Assert.assertEquals(testOut.getContent(), "Create database 'test_load_db_2' successfully.\n"
                + "Create table 'test_load_db_2.nullable_table' successfully.\n"
                + "Load table 'test_load_db_2.nullable_table' successfully, 17 rows have been inserted.\n",
                testOut.getContent());
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
                + "NULL, 1, 1, 1.1, 1.1, 11.11, 2023-06-01, 2023-06-01 10:10:10.111, 1, 1\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 1\n"
                + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL\n"
                + "col_boolean, col_integer, col_long, col_float, col_double, col_decimal, col_date, col_timestamp, col_string, col_fixed, col_binary\n"
                + "false, 2, 2, 2.2, 2.2, 12.12, 2023-06-02, 2023-06-02 10:10:10.111, 2, 2\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 2\n"
                + "false, 2, 2, 2.2, 2.2, 12.12, 2023-06-02, 2023-06-02 10:10:10.111, NULL, 2\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 2\n"
                + "false, 2, 2, 2.2, NULL, 12.12, 2023-06-02, 2023-06-02 10:10:10.111, 2, 2\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 2\n"
                + "false, 3, 3, 3.3, 3.3, 13.13, 2023-06-03, 2023-06-03 10:10:10.111, 3, 3\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 3\n"
                + "false, 3, 3, 3.3, 3.3, 13.13, 2023-06-03, 2023-06-03 10:10:10.111, 3, NULL, 3\n"
                + "false, 3, 3, 3.3, 3.3, NULL, 2023-06-03, 2023-06-03 10:10:10.111, 3, 3\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 3\n"
                + "true, 1, 1, 1.1, 1.1, 11.11, 2023-06-01, 2023-06-01 10:10:10.111, 1, 1\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 1\n"
                + "true, 1, 1, 1.1, 1.1, 11.11, 2023-06-01, NULL, 1, 1\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 1\n"
                + "true, 1, 1, NULL, 1.1, 11.11, 2023-06-01, 2023-06-01 10:10:10.111, 1, 1\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 1\n"
                + "true, 4, 4, 4.4, 4.4, 14.14, 2023-06-04, 2023-06-04 10:10:10.111, 4, 4\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, NULL\n"
                + "true, 4, 4, 4.4, 4.4, 14.14, NULL, 2023-06-04 10:10:10.111, 4, 4\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 4\n"
                + "true, 5, 5, 5.5, 5.5, 15.15, 2023-06-05, 2023-06-05 10:10:10.111, 5, 5\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 5\n"
                + "true, 5, 5, 5.5, 5.5, 15.15, 2023-06-05, 2023-06-05 10:10:10.111, 5, 5\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 5\n"
                + "true, 5, NULL, 5.5, 5.5, 15.15, 2023-06-05, 2023-06-05 10:10:10.111, 5, 5\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 5\n"
                + "true, NULL, 4, 4.4, 4.4, 14.14, 2023-06-04, 2023-06-04 10:10:10.111, 4, 4\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 4",
                sortedOutput);
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
        Assert.assertEquals(sortedOutput, "2023-06-01 10:10:10.111\n" + "2023-06-01 10:10:10.111\n"
                + "2023-06-01 10:10:10.111\n" + "2023-06-02 10:10:10.111\n"
                + "2023-06-02 10:10:10.111\n" + "2023-06-02 10:10:10.111\n"
                + "2023-06-03 10:10:10.111\n" + "2023-06-03 10:10:10.111\n"
                + "2023-06-03 10:10:10.111\n" + "2023-06-04 10:10:10.111\n"
                + "2023-06-04 10:10:10.111\n" + "2023-06-04 10:10:10.111\n"
                + "2023-06-05 10:10:10.111\n" + "2023-06-05 10:10:10.111\n"
                + "2023-06-05 10:10:10.111\n"
                + "Create database 'test_show_data_with_selected_fields_1' successfully.\n"
                + "Create table 'test_show_data_with_selected_fields_1.test_show_table_with_all_type' successfully.\n"
                + "Load table 'test_show_data_with_selected_fields_1.test_show_table_with_all_type' successfully, 17 rows have been inserted.\n"
                + "NULL\n" + "NULL\n" + "col_timestamp", sortedOutput);
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
        Assert.assertEquals(sortedOutput, "11.11, 2023-06-01 10:10:10.111\n"
                + "11.11, 2023-06-01 10:10:10.111\n" + "11.11, 2023-06-01 10:10:10.111\n"
                + "11.11, NULL\n" + "12.12, 2023-06-02 10:10:10.111\n"
                + "12.12, 2023-06-02 10:10:10.111\n" + "12.12, 2023-06-02 10:10:10.111\n"
                + "13.13, 2023-06-03 10:10:10.111\n" + "13.13, 2023-06-03 10:10:10.111\n"
                + "14.14, 2023-06-04 10:10:10.111\n" + "14.14, 2023-06-04 10:10:10.111\n"
                + "14.14, 2023-06-04 10:10:10.111\n" + "15.15, 2023-06-05 10:10:10.111\n"
                + "15.15, 2023-06-05 10:10:10.111\n" + "15.15, 2023-06-05 10:10:10.111\n"
                + "Create database 'test_show_data_with_selected_fields_2' successfully.\n"
                + "Create table 'test_show_data_with_selected_fields_2.test_show_table_with_all_type' successfully.\n"
                + "Load table 'test_show_data_with_selected_fields_2.test_show_table_with_all_type' successfully, 17 rows have been inserted.\n"
                + "NULL, 2023-06-03 10:10:10.111\n" + "NULL, NULL\n" + "col_decimal, col_timestamp",
                sortedOutput);
    }

    @Test
    public void testDeleteTableWithPositionDelete() throws Exception {
        final String dbName = "test_position_delete_db";
        final String tblName = "test_position_delete_table_with_all_type";
        createDatabase(dbName);
        createTable(dbName, tblName, "/load/nullable_load_types_template.json");
        loadTable(dbName, tblName, "/load/nullable_load_types.csv", 1, 4096, ',');
        showData(dbName, tblName, "col_int", "col_timestamp", "col_decimal");
        System.out.println("First Delete:");
        deleteData(dbName, tblName, FileContent.POSITION_DELETES, true, "col_integer=1");
        showData(dbName, tblName, "col_integer", "col_long", "col_double");
        System.out.println("Second Delete:");
        deleteData(dbName, tblName, FileContent.POSITION_DELETES, true, "col_integer=2",
                "col_long=3");
        showData(dbName, tblName, "col_integer", "col_long", "col_double");
        System.out.println("Third Delete:");
        deleteData(dbName, tblName, FileContent.POSITION_DELETES, true, "col_integer=3",
                "col_long=3");
        showData(dbName, tblName, "col_integer", "col_long", "col_double");
        System.out.println("Forth Delete:");
        deleteData(dbName, tblName, FileContent.POSITION_DELETES, false, "col_integer=4",
                "col_double=5.5");
        showData(dbName, tblName, "col_integer", "col_long", "col_double");

        Assert.assertEquals(testOut.getContent(),
                "Create database 'test_position_delete_db' successfully.\n"
                        + "Create table 'test_position_delete_db.test_position_delete_table_with_all_type' successfully.\n"
                        + "Load table 'test_position_delete_db.test_position_delete_table_with_all_type' successfully, 17 rows have been inserted.\n"
                        + "col_decimal, col_timestamp\n" + "1: 11.11, 2023-06-01 10:10:10.111\n"
                        + "2: 12.12, 2023-06-02 10:10:10.111\n"
                        + "3: 13.13, 2023-06-03 10:10:10.111\n"
                        + "4: 14.14, 2023-06-04 10:10:10.111\n"
                        + "5: 15.15, 2023-06-05 10:10:10.111\n"
                        + "6: 11.11, 2023-06-01 10:10:10.111\n"
                        + "7: 12.12, 2023-06-02 10:10:10.111\n"
                        + "8: NULL, 2023-06-03 10:10:10.111\n"
                        + "9: 14.14, 2023-06-04 10:10:10.111\n"
                        + "10: 15.15, 2023-06-05 10:10:10.111\n" + "11: 11.11, NULL\n"
                        + "12: 12.12, 2023-06-02 10:10:10.111\n"
                        + "13: 13.13, 2023-06-03 10:10:10.111\n"
                        + "14: 14.14, 2023-06-04 10:10:10.111\n"
                        + "15: 15.15, 2023-06-05 10:10:10.111\n" + "16: NULL, NULL\n"
                        + "17: 11.11, 2023-06-01 10:10:10.111\n" + "First Delete:\n"
                        + "Delete table (POSITION_DELETES) 'test_position_delete_db.test_position_delete_table_with_all_type' successfully.\n"
                        + "col_integer, col_long, col_double\n" + "1: 2, 2, 2.2\n"
                        + "2: 3, 3, 3.3\n" + "3: NULL, 4, 4.4\n" + "4: 5, NULL, 5.5\n"
                        + "5: 2, 2, NULL\n" + "6: 3, 3, 3.3\n" + "7: 4, 4, 4.4\n" + "8: 5, 5, 5.5\n"
                        + "9: 2, 2, 2.2\n" + "10: 3, 3, 3.3\n" + "11: 4, 4, 4.4\n"
                        + "12: 5, 5, 5.5\n" + "13: NULL, NULL, NULL\n" + "Second Delete:\n"
                        + "Delete table (POSITION_DELETES) 'test_position_delete_db.test_position_delete_table_with_all_type' successfully.\n"
                        + "col_integer, col_long, col_double\n" + "1: 2, 2, 2.2\n"
                        + "2: 3, 3, 3.3\n" + "3: NULL, 4, 4.4\n" + "4: 5, NULL, 5.5\n"
                        + "5: 2, 2, NULL\n" + "6: 3, 3, 3.3\n" + "7: 4, 4, 4.4\n" + "8: 5, 5, 5.5\n"
                        + "9: 2, 2, 2.2\n" + "10: 3, 3, 3.3\n" + "11: 4, 4, 4.4\n"
                        + "12: 5, 5, 5.5\n" + "13: NULL, NULL, NULL\n" + "Third Delete:\n"
                        + "Delete table (POSITION_DELETES) 'test_position_delete_db.test_position_delete_table_with_all_type' successfully.\n"
                        + "col_integer, col_long, col_double\n" + "1: 2, 2, 2.2\n"
                        + "2: NULL, 4, 4.4\n" + "3: 5, NULL, 5.5\n" + "4: 2, 2, NULL\n"
                        + "5: 4, 4, 4.4\n" + "6: 5, 5, 5.5\n" + "7: 2, 2, 2.2\n" + "8: 4, 4, 4.4\n"
                        + "9: 5, 5, 5.5\n" + "10: NULL, NULL, NULL\n" + "Forth Delete:\n"
                        + "Delete table (POSITION_DELETES) 'test_position_delete_db.test_position_delete_table_with_all_type' successfully.\n"
                        + "col_integer, col_long, col_double\n" + "1: 2, 2, 2.2\n"
                        + "2: NULL, 4, 4.4\n" + "3: 2, 2, NULL\n" + "4: 2, 2, 2.2\n"
                        + "5: NULL, NULL, NULL\n",
                testOut.getContent());
    }

    @Test
    public void testDeleteTableWithEqualityDelete() throws Exception {
        final String dbName = "test_equality_delete_db";
        final String tblName = "test_equality_delete_table_with_all_type";
        createDatabase(dbName);
        createTable(dbName, tblName, "/load/nullable_load_types_template.json");
        loadTable(dbName, tblName, "/load/nullable_load_types.csv", 1, 4096, ',');
        showData(dbName, tblName, "col_int", "col_timestamp", "col_decimal");
        System.out.println("First Delete:");
        deleteData(dbName, tblName, FileContent.EQUALITY_DELETES, true, "col_integer=1");
        showData(dbName, tblName, "col_integer", "col_long", "col_double");
        System.out.println("Second Delete:");
        deleteData(dbName, tblName, FileContent.EQUALITY_DELETES, true, "col_integer=2",
                "col_long=3");
        showData(dbName, tblName, "col_integer", "col_long", "col_double");
        System.out.println("Third Delete:");
        deleteData(dbName, tblName, FileContent.EQUALITY_DELETES, true, "col_integer=3",
                "col_long=3");
        showData(dbName, tblName, "col_integer", "col_long", "col_double");
        System.out.println("Forth Delete:");
        deleteData(dbName, tblName, FileContent.EQUALITY_DELETES, false, "col_integer=4",
                "col_double=5.5");
        showData(dbName, tblName, "col_integer", "col_long", "col_double");

        Assert.assertEquals(testOut.getContent(),
                "Create database 'test_equality_delete_db' successfully.\n"
                        + "Create table 'test_equality_delete_db.test_equality_delete_table_with_all_type' successfully.\n"
                        + "Load table 'test_equality_delete_db.test_equality_delete_table_with_all_type' successfully, 17 rows have been inserted.\n"
                        + "col_decimal, col_timestamp\n" + "1: 11.11, 2023-06-01 10:10:10.111\n"
                        + "2: 12.12, 2023-06-02 10:10:10.111\n"
                        + "3: 13.13, 2023-06-03 10:10:10.111\n"
                        + "4: 14.14, 2023-06-04 10:10:10.111\n"
                        + "5: 15.15, 2023-06-05 10:10:10.111\n"
                        + "6: 11.11, 2023-06-01 10:10:10.111\n"
                        + "7: 12.12, 2023-06-02 10:10:10.111\n"
                        + "8: NULL, 2023-06-03 10:10:10.111\n"
                        + "9: 14.14, 2023-06-04 10:10:10.111\n"
                        + "10: 15.15, 2023-06-05 10:10:10.111\n" + "11: 11.11, NULL\n"
                        + "12: 12.12, 2023-06-02 10:10:10.111\n"
                        + "13: 13.13, 2023-06-03 10:10:10.111\n"
                        + "14: 14.14, 2023-06-04 10:10:10.111\n"
                        + "15: 15.15, 2023-06-05 10:10:10.111\n" + "16: NULL, NULL\n"
                        + "17: 11.11, 2023-06-01 10:10:10.111\n" + "First Delete:\n"
                        + "Delete table (EQUALITY_DELETES) 'test_equality_delete_db.test_equality_delete_table_with_all_type' successfully.\n"
                        + "col_integer, col_long, col_double\n" + "1: 2, 2, 2.2\n"
                        + "2: 3, 3, 3.3\n" + "3: NULL, 4, 4.4\n" + "4: 5, NULL, 5.5\n"
                        + "5: 2, 2, NULL\n" + "6: 3, 3, 3.3\n" + "7: 4, 4, 4.4\n" + "8: 5, 5, 5.5\n"
                        + "9: 2, 2, 2.2\n" + "10: 3, 3, 3.3\n" + "11: 4, 4, 4.4\n"
                        + "12: 5, 5, 5.5\n" + "13: NULL, NULL, NULL\n" + "Second Delete:\n"
                        + "Delete table (EQUALITY_DELETES) 'test_equality_delete_db.test_equality_delete_table_with_all_type' successfully.\n"
                        + "col_integer, col_long, col_double\n" + "1: 2, 2, 2.2\n"
                        + "2: 3, 3, 3.3\n" + "3: NULL, 4, 4.4\n" + "4: 5, NULL, 5.5\n"
                        + "5: 2, 2, NULL\n" + "6: 3, 3, 3.3\n" + "7: 4, 4, 4.4\n" + "8: 5, 5, 5.5\n"
                        + "9: 2, 2, 2.2\n" + "10: 3, 3, 3.3\n" + "11: 4, 4, 4.4\n"
                        + "12: 5, 5, 5.5\n" + "13: NULL, NULL, NULL\n" + "Third Delete:\n"
                        + "Delete table (EQUALITY_DELETES) 'test_equality_delete_db.test_equality_delete_table_with_all_type' successfully.\n"
                        + "col_integer, col_long, col_double\n" + "1: 2, 2, 2.2\n"
                        + "2: NULL, 4, 4.4\n" + "3: 5, NULL, 5.5\n" + "4: 2, 2, NULL\n"
                        + "5: 4, 4, 4.4\n" + "6: 5, 5, 5.5\n" + "7: 2, 2, 2.2\n" + "8: 4, 4, 4.4\n"
                        + "9: 5, 5, 5.5\n" + "10: NULL, NULL, NULL\n" + "Forth Delete:\n"
                        + "Delete table (EQUALITY_DELETES) 'test_equality_delete_db.test_equality_delete_table_with_all_type' successfully.\n"
                        + "col_integer, col_long, col_double\n" + "1: 2, 2, 2.2\n"
                        + "2: NULL, 4, 4.4\n" + "3: 2, 2, NULL\n" + "4: 2, 2, 2.2\n"
                        + "5: NULL, NULL, NULL\n",
                testOut.getContent());
    }

    @Test
    public void testDeleteTableWithEqualityDeleteForNonNullableColumn() throws Exception {
        final String dbName = "test_equality_delete_db";
        final String tblName = "test_equality_delete_table_with_all_type";
        createDatabase(dbName);
        createTable(dbName, tblName, "/load/non_nullable_load_types_template.json");
        loadTable(dbName, tblName, "/load/non_nullable_load_types.csv", 1, 4096, ',');
        showData(dbName, tblName);
        System.out.println("First Delete:");
        deleteData(dbName, tblName, FileContent.EQUALITY_DELETES, true, "col_date=2023-06-01");
        showData(dbName, tblName);
        System.out.println("Second Delete:");
        deleteData(dbName, tblName, FileContent.EQUALITY_DELETES, true, "col_integer=2",
                "col_long=3");
        showData(dbName, tblName);
        System.out.println("Third Delete:");
        deleteData(dbName, tblName, FileContent.EQUALITY_DELETES, true, "col_integer=3",
                "col_string=3");
        showData(dbName, tblName);
        System.out.println("Forth Delete:");
        deleteData(dbName, tblName, FileContent.EQUALITY_DELETES, false, "col_double=4.4",
                "col_timestamp=2023-06-02 10:10:10.111111");
        showData(dbName, tblName);

        Assert.assertEquals(testOut.getContent(),
                "Create database 'test_equality_delete_db' successfully.\n"
                        + "Create table 'test_equality_delete_db.test_equality_delete_table_with_all_type' successfully.\n"
                        + "Load table 'test_equality_delete_db.test_equality_delete_table_with_all_type' successfully, 5 rows have been inserted.\n"
                        + "col_boolean, col_integer, col_long, col_float, col_double, col_decimal, col_date, col_timestamp, col_string, col_fixed, col_rbinary\n"
                        + "1: true, 1, 1, 1.1, 1.1, 11.11, 2023-06-01, 2023-06-01 10:10:10.111, 1, 1\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 1\n"
                        + "2: false, 2, 2, 2.2, 2.2, 12.12, 2023-06-02, 2023-06-02 10:10:10.111, 2, 2\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 2\n"
                        + "3: false, 3, 3, 3.3, 3.3, 13.13, 2023-06-03, 2023-06-03 10:10:10.111, 3, 3\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 3\n"
                        + "4: true, 4, 4, 4.4, 4.4, 14.14, 2023-06-04, 2023-06-04 10:10:10.111, 4, 4\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 4\n"
                        + "5: true, 5, 5, 5.5, 5.5, 15.15, 2023-06-05, 2023-06-05 10:10:10.111, 5, 5\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 5\n"
                        + "First Delete:\n"
                        + "Delete table (EQUALITY_DELETES) 'test_equality_delete_db.test_equality_delete_table_with_all_type' successfully.\n"
                        + "col_boolean, col_integer, col_long, col_float, col_double, col_decimal, col_date, col_timestamp, col_string, col_fixed, col_rbinary\n"
                        + "1: false, 2, 2, 2.2, 2.2, 12.12, 2023-06-02, 2023-06-02 10:10:10.111, 2, 2\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 2\n"
                        + "2: false, 3, 3, 3.3, 3.3, 13.13, 2023-06-03, 2023-06-03 10:10:10.111, 3, 3\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 3\n"
                        + "3: true, 4, 4, 4.4, 4.4, 14.14, 2023-06-04, 2023-06-04 10:10:10.111, 4, 4\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 4\n"
                        + "4: true, 5, 5, 5.5, 5.5, 15.15, 2023-06-05, 2023-06-05 10:10:10.111, 5, 5\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 5\n"
                        + "Second Delete:\n"
                        + "Delete table (EQUALITY_DELETES) 'test_equality_delete_db.test_equality_delete_table_with_all_type' successfully.\n"
                        + "col_boolean, col_integer, col_long, col_float, col_double, col_decimal, col_date, col_timestamp, col_string, col_fixed, col_rbinary\n"
                        + "1: false, 2, 2, 2.2, 2.2, 12.12, 2023-06-02, 2023-06-02 10:10:10.111, 2, 2\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 2\n"
                        + "2: false, 3, 3, 3.3, 3.3, 13.13, 2023-06-03, 2023-06-03 10:10:10.111, 3, 3\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 3\n"
                        + "3: true, 4, 4, 4.4, 4.4, 14.14, 2023-06-04, 2023-06-04 10:10:10.111, 4, 4\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 4\n"
                        + "4: true, 5, 5, 5.5, 5.5, 15.15, 2023-06-05, 2023-06-05 10:10:10.111, 5, 5\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 5\n"
                        + "Third Delete:\n"
                        + "Delete table (EQUALITY_DELETES) 'test_equality_delete_db.test_equality_delete_table_with_all_type' successfully.\n"
                        + "col_boolean, col_integer, col_long, col_float, col_double, col_decimal, col_date, col_timestamp, col_string, col_fixed, col_rbinary\n"
                        + "1: false, 2, 2, 2.2, 2.2, 12.12, 2023-06-02, 2023-06-02 10:10:10.111, 2, 2\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 2\n"
                        + "2: true, 4, 4, 4.4, 4.4, 14.14, 2023-06-04, 2023-06-04 10:10:10.111, 4, 4\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 4\n"
                        + "3: true, 5, 5, 5.5, 5.5, 15.15, 2023-06-05, 2023-06-05 10:10:10.111, 5, 5\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 5\n"
                        + "Forth Delete:\n"
                        + "Delete table (EQUALITY_DELETES) 'test_equality_delete_db.test_equality_delete_table_with_all_type' successfully.\n"
                        + "col_boolean, col_integer, col_long, col_float, col_double, col_decimal, col_date, col_timestamp, col_string, col_fixed, col_rbinary\n"
                        + "1: true, 5, 5, 5.5, 5.5, 15.15, 2023-06-05, 2023-06-05 10:10:10.111, 5, 5\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000, 5\n",
                testOut.getContent());
    }
}
