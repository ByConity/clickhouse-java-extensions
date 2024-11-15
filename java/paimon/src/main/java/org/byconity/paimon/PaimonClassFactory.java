package org.byconity.paimon;

import org.byconity.common.loader.ClassFactory;

import java.util.Map;

public class PaimonClassFactory extends ClassFactory {
    @Override
    protected String getModuleName() {
        return "paimon";
    }

    public PaimonClassFactory(Map<String, String> options) {
        super(options);
    }
}
