package org.byconity.common;

public abstract class ColumnVector implements AutoCloseable {

    protected ColumnType type;

    protected ColumnVector(ColumnType type) {
        this.type = type;
    }

    public final ColumnType dataType() {
        return type;
    }

    @Override
    public abstract void close();

}
