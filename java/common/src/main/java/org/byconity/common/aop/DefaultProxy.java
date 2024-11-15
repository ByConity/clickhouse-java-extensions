package org.byconity.common.aop;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.byconity.common.loader.ThreadContextClassLoader;
import org.byconity.common.metaclient.MetaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Map;

public class DefaultProxy implements MethodInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultProxy.class);
    private static final Gson GSON = new GsonBuilder().create();

    @SuppressWarnings("unchecked")
    public static <T extends MetaClient> T wrap(Class<?> clazz, Class<?>[] argumentTypes,
            Object[] arguments) {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(clazz);
        enhancer.setCallback(new DefaultProxy());
        return (T) enhancer.create(argumentTypes, arguments);
    }

    @Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy)
            throws Throwable {
        long start = System.currentTimeMillis();
        Throwable throwable = null;
        Class<?>[] parameterTypes = method.getParameterTypes();
        String[] parameterTypeStrs = new String[parameterTypes.length];
        String[] parameterValueStrs = new String[parameterTypes.length];
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(obj.getClass().getClassLoader())) {
            parseArgs(args, parameterTypes, parameterTypeStrs, parameterValueStrs);
            return proxy.invokeSuper(obj, args);
        } catch (Throwable e) {
            throwable = e;
            throw e;
        } finally {
            long useTime = System.currentTimeMillis() - start;
            if (throwable == null) {
                LOGGER.info("method: {}, argTypes: {}, args: {}, cost: {}ms", method.getName(),
                        parameterTypeStrs, parameterValueStrs, useTime);
            } else {
                LOGGER.error("method: {}, argTypes: {}, args: {}, cost: {}ms", method.getName(),
                        parameterTypeStrs, parameterValueStrs, useTime, throwable);
            }
        }
    }

    private static void parseArgs(Object[] args, Class<?>[] parameterTypes,
            String[] parameterTypeStrs, String[] parameterValueStrs) {
        try {
            for (int i = 0; i < parameterTypes.length; i++) {
                Class<?> parameterType = parameterTypes[i];
                parameterTypeStrs[i] = parameterType.getSimpleName();
                if (args[i] == null) {
                    parameterValueStrs[i] = "null";
                } else if (isPrimitiveOrStringType(parameterType)) {
                    parameterValueStrs[i] = args[i].toString();
                } else if (parameterType.equals(Map.class)) {
                    Map<?, ?> map = (Map<?, ?>) args[i];
                    if (map.keySet().stream().allMatch(DefaultProxy::isPrimitiveOrStringValue)
                            && map.values().stream()
                                    .allMatch(DefaultProxy::isPrimitiveOrStringValue)) {
                        parameterValueStrs[i] = GSON.toJson(map);
                    } else {
                        parameterValueStrs[i] = "<omitted>";
                    }
                } else {
                    parameterValueStrs[i] = "<omitted>";
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Failed to parse args", e);
        }
    }

    private static boolean isPrimitiveOrStringType(Class<?> clazz) {
        return clazz.isPrimitive() || clazz.equals(String.class);
    }

    private static boolean isPrimitiveOrStringValue(Object obj) {
        return obj.getClass().isPrimitive() || obj.getClass().equals(String.class);
    }
}
