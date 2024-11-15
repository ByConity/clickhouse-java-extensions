package org.byconity.las;

import org.byconity.common.loader.ClassFactory;

import java.util.Map;

public class LasClassFactory extends ClassFactory {
    @Override
    protected String getModuleName() {
        return "las";
    }

    public LasClassFactory(Map<String, String> options) {
        super(options);
    }
}
