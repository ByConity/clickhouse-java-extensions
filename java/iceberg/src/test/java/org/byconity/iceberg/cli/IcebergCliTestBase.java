package org.byconity.iceberg.cli;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.UUID;

public class IcebergCliTestBase {
    protected static final Gson GSON = new GsonBuilder().create();
    protected static final String PATH = "/tmp/iceberg_ut";
    protected static final String TMP_PATH = PATH + "/tmp/";
    protected static final String LOCAL_CATALOG_PARAMS = String.format(
            "{\"metastoreType\":\"hadoop\",\"filesystemType\": \"HDFS\", \"warehouse\":\"file://%s/warehouse\"}",
            PATH);
    protected static PrintStream previousStdOut;
    protected static PrintStream previousStdErr;
    protected static final TestPrinter testOut = new TestPrinter(new ByteArrayOutputStream());
    protected static String helpMessage;

    @BeforeClass
    public static void beforeClass() {
        // Init logger before any test cases in order to avoid the SLFJ4 binding log
        // pollute the test cases' standard output.
        Logger logger = LoggerFactory.getLogger(IcebergCliTestBase.class);
        logger.info("test");

        LoadTableProcessor.printBatchInfo = false;
        Configurator.setRootLevel(org.apache.logging.log4j.Level.OFF);
        previousStdOut = System.out;
        previousStdErr = System.err;

        System.setOut(testOut);
        System.setErr(testOut);

        IcebergCli.main(new String[] {"--help"});
        helpMessage = testOut.getContent();
        testOut.reset();
    }

    @AfterClass
    public static void afterClass() {
        System.setOut(previousStdOut);
        System.setErr(previousStdErr);
    }

    @Before
    public void before() throws Exception {
        FileUtils.deleteDirectory(new File(PATH));
        File tempFile = new File(getTmpFilePath());
        boolean res = tempFile.mkdirs();
        Assert.assertTrue(res);
    }

    @After
    public void after() throws Exception {
        FileUtils.deleteDirectory(new File(PATH));
        testOut.reset();
    }

    protected static String getTmpFilePath() {
        return TMP_PATH + UUID.randomUUID();
    }

    protected void showDatabase() {
        IcebergCli.main(new String[] {"--catalog", LOCAL_CATALOG_PARAMS, "--action",
                ActionType.SHOW_DATABASES.name()});
    }

    protected void createDatabase(String database) {
        IcebergCli.main(new String[] {"--catalog", LOCAL_CATALOG_PARAMS, "--action",
                ActionType.CREATE_DATABASE.name(), "--arg", database, "--verbose"});
    }

    protected void dropDatabase(String database) {
        IcebergCli.main(new String[] {"--catalog", LOCAL_CATALOG_PARAMS, "--action",
                ActionType.DROP_DATABASE.name(), "--arg", database, "--verbose"});
    }

    protected void showTables(String database) {
        IcebergCli.main(new String[] {"--catalog", LOCAL_CATALOG_PARAMS, "--action",
                ActionType.SHOW_TABLES.name(), "--arg", database, "--verbose"});
    }

    protected void createTable(String database, String table, String templatePath)
            throws Exception {
        createTableWithFormat(database, table, FileFormat.PARQUET, templatePath);
    }

    protected void createTableWithFormat(String database, String table, FileFormat format,
            String templatePath) throws Exception {
        InputStream resourceAsStream = getClass().getResourceAsStream(templatePath);
        Preconditions.checkState(resourceAsStream != null);
        String template = IOUtils.toString(resourceAsStream, Charset.defaultCharset());
        String content = String.format(template, database, table, format.name());
        String tmpFilePath = getTmpFilePath();
        IOUtils.copy(IOUtils.toInputStream(content, Charset.defaultCharset()),
                FileUtils.openOutputStream(new File(tmpFilePath)));
        IcebergCli.main(new String[] {"--catalog", LOCAL_CATALOG_PARAMS, "--action",
                ActionType.CREATE_TABLE.name(), "--arg", tmpFilePath, "--verbose"});
    }

    protected void showTableSchema(String database, String table) {
        IcebergCli.main(new String[] {"--catalog", LOCAL_CATALOG_PARAMS, "--action",
                ActionType.SHOW_TABLE_SCHEMA.name(), "--arg", database + "." + table, "--verbose"});
    }

    protected void dropTable(String database, String table) {
        IcebergCli.main(new String[] {"--catalog", LOCAL_CATALOG_PARAMS, "--action",
                ActionType.DROP_TABLE.name(), "--arg", database + "." + table, "--verbose"});
    }

    protected void populateTable(String database, String table, int rows) {
        String params = String.format("{\"database\":\"%s\",\"table\":\"%s\",\"rows\":%d}",
                database, table, rows);
        IcebergCli.main(new String[] {"--catalog", LOCAL_CATALOG_PARAMS, "--action",
                ActionType.POPULATE_TABLE.name(), "--arg", params, "--verbose"});
    }

    protected void loadTable(String database, String table, String csvPath, int maxRowsPerFile,
            char delimiter) throws Exception {
        InputStream resourceAsStream = getClass().getResourceAsStream(csvPath);
        Preconditions.checkState(resourceAsStream != null);
        String tmpFilePath = getTmpFilePath();
        IOUtils.copy(resourceAsStream, FileUtils.openOutputStream(new File(tmpFilePath)));
        IcebergCli.main(new String[] {"--catalog", LOCAL_CATALOG_PARAMS, "--action",
                ActionType.LOAD_TABLE.name(), "--arg",
                String.format(
                        "{\"database\":\"%s\",\"table\":\"%s\",\"filePath\":\"%s\",\"maxRowsPerFile\":%d,\"delimiter\":\"%c\"}",
                        database, table, tmpFilePath, maxRowsPerFile, delimiter),
                "--verbose"});
    }

    protected void loadTable(String database, String table, String csvPath, int parallelism,
            int maxRowsPerFile, char delimiter) throws Exception {
        InputStream resourceAsStream = getClass().getResourceAsStream(csvPath);
        Preconditions.checkState(resourceAsStream != null);
        String tmpFilePath = getTmpFilePath();
        IOUtils.copy(resourceAsStream, FileUtils.openOutputStream(new File(tmpFilePath)));
        IcebergCli.main(new String[] {"--catalog", LOCAL_CATALOG_PARAMS, "--action",
                ActionType.LOAD_TABLE.name(), "--arg",
                String.format(
                        "{\"database\":\"%s\",\"table\":\"%s\",\"filePath\":\"%s\",\"parallelism\":%d,\"maxRowsPerFile\":%d,\"delimiter\":\"%c\"}",
                        database, table, tmpFilePath, parallelism, maxRowsPerFile, delimiter),
                "--verbose"});
    }

    protected void showData(String database, String table, String... fieldNames) throws Exception {
        if (fieldNames.length == 0) {
            IcebergCli.main(new String[] {"--catalog", LOCAL_CATALOG_PARAMS, "--action",
                    ActionType.SHOW_DATA.name(), "--arg", database + "." + table, "--verbose"});
        } else {
            IcebergCli.main(new String[] {"--catalog", LOCAL_CATALOG_PARAMS, "--action",
                    ActionType.SHOW_DATA.name(), "--arg",
                    database + "." + table + ".{" + String.join(",", fieldNames) + "}",
                    "--verbose"});
        }
    }

    protected void deleteData(String database, String table, FileContent deleteType, boolean isAnd,
            String... predicates) throws Exception {
        IcebergCli.main(new String[] {"--catalog", LOCAL_CATALOG_PARAMS, "--action",
                ActionType.DELETE_TABLE.name(), "--arg",
                String.format(
                        "{\"database\":\"%s\",\"table\":\"%s\",\"deleteType\":\"%s\",\"isAnd\":%b,\"predicates\":%s}",
                        database, table, deleteType.name(), isAnd, GSON.toJson(predicates),
                        "--verbose")});
    }
}
