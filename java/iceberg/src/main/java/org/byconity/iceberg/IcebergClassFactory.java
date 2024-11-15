package org.byconity.iceberg;

import org.byconity.common.loader.ClassFactory;

import java.util.Map;

public class IcebergClassFactory extends ClassFactory {

    @Override
    protected String getModuleName() {
        return "iceberg";
    }

    public IcebergClassFactory(Map<String, String> options) {
        super(options);
    }
}
