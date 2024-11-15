package org.byconity.paimon.util;

import org.apache.paimon.types.DataType;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class ReflectionsUtils {
    public static List<Class<?>> getAllSubclasses(String packageName, Class<?> parentClass)
            throws Exception {
        List<Class<?>> subclasses = new ArrayList<>();
        String path = packageName.replace('.', '/');
        Enumeration<URL> resources =
                Thread.currentThread().getContextClassLoader().getResources(path);

        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
            if (resource.getProtocol().equals("file")) {
                subclasses
                        .addAll(findClasses(new File(resource.toURI()), packageName, parentClass));
            } else if (resource.getProtocol().equals("jar")) {
                subclasses.addAll(findClassesInJar(resource, packageName, parentClass));
            }
        }
        return subclasses;
    }

    private static List<Class<?>> findClasses(File directory, String packageName,
            Class<?> parentClass) throws ClassNotFoundException {
        List<Class<?>> classes = new ArrayList<>();
        if (!directory.exists()) {
            return classes;
        }
        File[] files = directory.listFiles();
        assert files != null;
        for (File file : files) {
            if (file.isDirectory()) {
                classes.addAll(findClasses(file, packageName + "." + file.getName(), parentClass));
            } else if (file.getName().endsWith(".class")) {
                Class<?> clazz = Class.forName(packageName + '.'
                        + file.getName().substring(0, file.getName().length() - 6));
                if (parentClass.isAssignableFrom(clazz) && !clazz.equals(parentClass)
                        && !clazz.isInterface()
                        && !java.lang.reflect.Modifier.isAbstract(clazz.getModifiers())) {
                    classes.add(clazz);
                }
            }
        }
        return classes;
    }

    private static List<Class<?>> findClassesInJar(URL resource, String packageName,
            Class<?> parentClass) throws IOException, ClassNotFoundException {
        List<Class<?>> classes = new ArrayList<>();
        String jarPath = resource.getPath().substring(5, resource.getPath().indexOf("!"));
        jarPath = URLDecoder.decode(jarPath, "UTF-8");
        JarFile jarFile = new JarFile(jarPath);
        Enumeration<JarEntry> entries = jarFile.entries();
        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            String name = entry.getName();
            if (name.startsWith(packageName.replace('.', '/')) && name.endsWith(".class")) {
                String className = name.replace('/', '.').substring(0, name.length() - 6);
                Class<?> clazz = Class.forName(className);
                if (parentClass.isAssignableFrom(clazz) && !clazz.equals(parentClass)
                        && !clazz.isInterface()
                        && !java.lang.reflect.Modifier.isAbstract(clazz.getModifiers())) {
                    classes.add(clazz);
                }
            }
        }
        jarFile.close();
        return classes;
    }

    public static void main(String[] args) throws Exception {
        List<Class<?>> implementations =
                getAllSubclasses("org.apache.paimon.types", DataType.class);
        for (Class<?> impl : implementations) {
            System.out.println(impl.getName());
        }
    }
}
