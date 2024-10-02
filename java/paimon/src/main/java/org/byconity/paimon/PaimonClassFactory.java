package org.byconity.paimon;

import org.byconity.common.loader.ClassFactory;

public class PaimonClassFactory extends ClassFactory {
    @Override
    protected String getModuleName() {
        return "paimon";
    }
}
