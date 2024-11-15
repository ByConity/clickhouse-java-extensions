package org.byconity.iceberg.cli;

import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.catalog.Catalog;
import org.byconity.iceberg.params.IcebergParams;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;

public class IcebergCli {
    private static final String ENV_NAME_CATALOG = "ICEBERG_CATALOG";
    private static CommandLine cmd;
    private static HelpFormatter formatter;
    private static Options options;
    private static Option optionHelp;
    private static Option optionVerbose;
    private static Option optionCatalog;
    private static Option optionAction;
    private static Option optionArg;
    private static Catalog catalog;

    private static void stop() {
        throw new StopException();
    }

    private static void parseOptions(String[] args) {
        options = new Options();

        optionHelp = new Option(null, "help", false, "Show help document.");
        optionHelp.setRequired(false);
        options.addOption(optionHelp);

        optionVerbose = new Option(null, "verbose", false, "Show Error Stack.");
        optionVerbose.setRequired(false);
        options.addOption(optionVerbose);

        optionCatalog = new Option(null, "catalog", true, String.format(
                "catalog params json or filepath contains params json, or you can config it through env variable '%s'.",
                ENV_NAME_CATALOG));
        optionCatalog.setRequired(false);
        options.addOption(optionCatalog);

        optionAction = new Option(null, "action", true,
                String.format("Valid actions: <%s>.", ActionType.OptionString()));
        optionAction.setRequired(false);
        options.addOption(optionAction);

        optionArg = new Option(null, "arg", true, "arg for action, can be text of filepath.");
        optionArg.setRequired(false);
        options.addOption(optionArg);

        CommandLineParser parser = new DefaultParser();
        formatter = new HelpFormatter();

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("IcebergCli", options);
            stop();
        }

        if (cmd.hasOption(optionHelp)) {
            formatter.printHelp("IcebergCli", options);
            stop();
        }
    }

    private static void parseCatalog() throws Exception {
        String catalogValue = System.getenv(ENV_NAME_CATALOG);
        if (catalogValue == null) {
            checkOrPrintHelp(cmd.hasOption(optionCatalog), String.format(
                    "--catalog is mandatory if env variable '%s' is not set.", ENV_NAME_CATALOG));
            catalogValue = cmd.getOptionValue(optionCatalog);
        }
        File file = new File(catalogValue);
        String content;
        if (file.exists() && file.isFile()) {
            content =
                    IOUtils.toString(Files.newInputStream(file.toPath()), Charset.defaultCharset());
        } else {
            content = catalogValue;
        }
        IcebergParams params = IcebergParams.parseFrom(content);
        catalog = params.getCatalog();
    }

    private static void processAction() throws Exception {
        checkOrPrintHelp(cmd.hasOption(optionAction), "Please use --action to specify action.");
        String optionValue = cmd.getOptionValue(optionAction);
        ActionType actionType = ActionType.of(optionValue);
        checkOrPrintHelp(actionType != null, "Wrong action type.");
        switch (actionType) {
            case SHOW_DATABASES:
                ShowDatabaseProcessor.process(catalog);
                break;
            case CREATE_DATABASE:
                CreateDatabaseProcessor.process(catalog, parseActionArg(actionType));
                break;
            case DROP_DATABASE:
                DropDatabaseProcessor.process(catalog, parseActionArg(actionType));
                break;
            case SHOW_TABLES:
                ShowTableProcessor.process(catalog, parseActionArg(actionType));
                break;
            case CREATE_TABLE:
                CreateTableProcessor.process(catalog, parseActionArg(actionType));
                break;
            case SHOW_TABLE_SCHEMA:
                ShowTableSchemaProcessor.process(catalog, parseActionArg(actionType));
                break;
            case DROP_TABLE:
                DropTableProcessor.process(catalog, parseActionArg(actionType));
                break;
            case POPULATE_TABLE:
                PopulateTableProcessor.process(catalog, parseActionArg(actionType));
                break;
            case LOAD_TABLE:
                LoadTableProcessor.process(catalog, parseActionArg(actionType));
                break;
            case SHOW_DATA:
                ShowDataProcessor.process(catalog, parseActionArg(actionType));
                break;
            case DELETE_TABLE:
                DeleteTableProcessor.process(catalog, parseActionArg(actionType));
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Action '%s' is unsupported yet.", actionType.name()));
        }
    }

    private static String parseActionArg(ActionType actionType) throws Exception {
        checkOrPrintHelp(cmd.hasOption(optionArg),
                String.format("--arg is mandatory if you use --action '%s'", actionType.name()));
        String optionValue = cmd.getOptionValue(optionArg);
        File file = new File(optionValue);
        if (file.exists() && file.isFile()) {
            return IOUtils.toString(Files.newInputStream(file.toPath()), Charset.defaultCharset());
        } else {
            return optionValue;
        }
    }

    private static void checkOrPrintHelp(boolean expr, String message) {
        if (!expr) {
            throw new PrintHelpException(message);
        }
    }

    public static void main(String[] args) {
        try {
            parseOptions(args);
            parseCatalog();
            processAction();
        } catch (StopException e) {
            // ignore
        } catch (PrintHelpException e) {
            System.err.println(e.getMessage());
            formatter.printHelp("IcebergCli", options);
        } catch (Throwable e) {
            if (cmd != null && cmd.hasOption(optionVerbose)) {
                e.printStackTrace();
            } else {
                System.err.println(e.getMessage());
            }
        }
    }

    private static class StopException extends RuntimeException {

    }


    private static class PrintHelpException extends RuntimeException {
        public PrintHelpException(String message) {
            super(message);
        }
    }
}

