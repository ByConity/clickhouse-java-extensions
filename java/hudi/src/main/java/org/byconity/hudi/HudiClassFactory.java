package org.byconity.hudi;

import org.byconity.common.loader.ClassFactory;

import java.util.Map;

public class HudiClassFactory extends ClassFactory {
    @Override
    protected String getModuleName() {
        return "hudi";
    }

    public HudiClassFactory(Map<String, String> options) {
        super(options);
    }
}
