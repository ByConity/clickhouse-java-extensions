package org.byconity.common.loader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.byconity.common.util.ExceptionUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

/**
 * This class is loaded through system-level classloader, each module will have its own instance of
 * this class. DO NOT perform init process in static way, make it instance-level to avoid conflicts
 * between different modules.
 *
 * The ONLY thing you can do and should with this class is to get class via getClass method.
 * Otherwise, you may brake the isolation context created by module classloader.
 */
public abstract class ClassFactory {

    protected ModuleClassLoader classLoader;

    protected ClassFactory() {
        try {
            classLoader = ModuleClassLoader.create(getModuleName());
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }

    /**
     * Name of module
     */
    protected abstract String getModuleName();

    /**
     * Entry to get class of current module
     */
    public final Class<?> getClass(String className) throws ClassNotFoundException {
        return classLoader.loadClass(className);
    }

    /**
     * Initialize the isolated context of this module
     */
    public final void initModuleContext() throws Exception {
        initLog4j2();
    }

    /**
     * This method is used to initialize the isolated context of log4j2, avoiding conflict between
     * different modules.
     */
    private void initLog4j2() throws Exception {
        Class<?> clazz =
                getClass("org.byconity.common.loader.ClassFactory$Log4jContextInitializer");
        clazz.getMethod("init", String.class).invoke(null, getModuleName());
    }

    public static class Log4jContextInitializer {
        public static void init(String moduleName) throws IOException {
            URL resource = Log4jContextInitializer.class.getClassLoader()
                    .getResource(String.format("%s_log4j2.xml", moduleName));
            if (resource == null) {
                throw new FileNotFoundException(
                        String.format("Cannot find log4j2.xml in module %s", moduleName));
            }
            ConfigurationSource source = new ConfigurationSource(resource.openStream(), resource);
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            Configuration config = new XmlConfiguration(context, source);
            context.start(config);
        }
    }
}
