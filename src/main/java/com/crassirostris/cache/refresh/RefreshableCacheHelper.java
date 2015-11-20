package com.crassirostris.cache.refresh;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.springframework.beans.BeanUtils;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.ApplicationContext;
import org.thymeleaf.util.ArrayUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;

/**
 * RefreshableCacheHelper
 * create new instance that setted field (get BeanFactory)
 * User: crassirostris
 * Date: 2015-10-14
 * Time: 오후 6:04
 */
@Slf4j
public class RefreshableCacheHelper {
	private RefreshableCacheHelper() {
	}

	private static Set<Method> methodsAnnotatedWith;

	private static void setMethodsAnnotatedWith() {
		if (methodsAnnotatedWith == null) {
			Reflections reflections = new Reflections("", new MethodAnnotationsScanner());
			methodsAnnotatedWith = reflections.getMethodsAnnotatedWith(Cacheable.class);
		}
	}

	protected static Object excuteTargetMethod(ApplicationContext ctx, Object key, String name) {
		Method targetMethod = findTargetMethod(name);
		Object result = null;
		if (targetMethod == null) {
			if (log.isDebugEnabled()) {
				log.debug(name + " is none targetClass with refresh");
			}
			return result;
		}

		try {
			Object instance = getInstanceWithFields(targetMethod.getDeclaringClass(), ctx);
			Object[] keys = (Object[]) FieldUtils.readDeclaredField(key, "params", true);
			result = targetMethod.invoke(instance, keys);
		} catch (IllegalAccessException | InvocationTargetException | IllegalArgumentException | InstantiationException e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	private static Object getInstanceWithFields(Class<?> targetMethod, ApplicationContext ctx) throws IllegalAccessException, InstantiationException {
		if (targetMethod.isPrimitive()) {
			return null;
		}
		Object instance = targetMethod.newInstance();
		Object targetBean = ctx.getBean(targetMethod);
		BeanUtils.copyProperties(targetBean, instance);
		return instance;
	}

	protected static Method findTargetMethod(String name) {
		setMethodsAnnotatedWith();
		for (Method method : methodsAnnotatedWith) {
			Cacheable annotation = method.getAnnotation(Cacheable.class);
			String[] value = annotation.value();
			if (ArrayUtils.contains(value, name)) {
				return method;
			}
		}
		return null;
	}

	protected static Class getTargetMethodReturnClase(String name) {
		Method targetMethod = findTargetMethod(name);
		return targetMethod.getReturnType();
	}
}
