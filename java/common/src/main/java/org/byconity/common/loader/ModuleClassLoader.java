package org.byconity.common.loader;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;

public class ModuleClassLoader extends URLClassLoader {
    static {
        ClassLoader.registerAsParallelCapable();
    }

    public static ModuleClassLoader create(String moduleName) throws MalformedURLException {
        String jarNameSuffix = String.format("%s-reader-jar-with-dependencies.jar", moduleName);
        String classpath = System.getProperty("java.class.path");
        String[] moduleJarFiles = classpath.split(":");
        String targetJarFile = null;
        for (String jarFile : moduleJarFiles) {
            if (jarFile.endsWith(jarNameSuffix)) {
                targetJarFile = jarFile;
                break;
            }
        }
        if (targetJarFile == null) {
            throw new RuntimeException(
                    String.format("Cannot find '%s' in classpath '%s'", jarNameSuffix, classpath));
        }

        return new ModuleClassLoader(new URL[] {new File(targetJarFile).toURI().toURL()});
    }

    private final File jarFile;
    private final ClassLoaderWrapper parent;

    private ModuleClassLoader(URL[] urls) {
        super(urls, null);
        this.jarFile = new File(urls[0].getPath());
        this.parent = new ClassLoaderWrapper(ClassLoader.getSystemClassLoader());
    }

    public File getJarFile() {
        return jarFile;
    }

    private boolean isValidParentResource(URL url) {
        return url != null && !url.getPath().contains("-reader-jar-with-dependencies.jar");
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        try {
            return super.loadClass(name, resolve);
        } catch (ClassNotFoundException cnf) {
            return parent.loadClass(name, resolve);
        }
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        // Load resource from current module classLoader
        List<URL> urls = Collections.list(super.getResources(name));
        // Load resource from parent classLoader but exclude other module resources
        urls.addAll(Collections.list(parent.getResources(name)).stream()
                .filter(this::isValidParentResource).collect(Collectors.toList()));
        return Collections.enumeration(urls);
    }

    @Override
    public URL getResource(String name) {
        // Load resource from current module classLoader
        URL url = super.getResource(name);
        if (url == null) {
            // Load resource from parent classLoader but exclude other module resources
            url = parent.getResource(name);
            if (!isValidParentResource(url)) {
                return null;
            }
        }
        return url;
    }


    /**
     * The only function of this wrapper is changing access modifiers of loadClass from protected to
     * public
     */
    private static final class ClassLoaderWrapper extends ClassLoader {
        static {
            ClassLoader.registerAsParallelCapable();
        }

        public ClassLoaderWrapper(ClassLoader parent) {
            super(parent);
        }

        @Override
        public Class<?> findClass(String name) throws ClassNotFoundException {
            return super.findClass(name);
        }

        @Override
        public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            return super.loadClass(name, resolve);
        }
    }
}
