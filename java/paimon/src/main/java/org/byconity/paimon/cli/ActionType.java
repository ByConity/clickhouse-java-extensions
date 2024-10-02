package org.byconity.paimon.cli;

public enum ActionType {
    SHOW_DATABASES, CREATE_DATABASE, DROP_DATABASE, SHOW_TABLES, CREATE_TABLE, DROP_TABLE, SHOW_TABLE_SCHEMA, POPULATE_TABLE, LOAD_TABLE, SHOW_DATA;

    public static String OptionString() {
        StringBuilder buffer = new StringBuilder();
        for (ActionType actionType : ActionType.values()) {
            buffer.append(actionType.name()).append("|");
        }
        return buffer.substring(0, buffer.length() - 1);
    }

    public static ActionType of(String action) {
        for (ActionType actionType : ActionType.values()) {
            if (actionType.name().equalsIgnoreCase(action)) {
                return actionType;
            }
        }
        return null;
    }
}
