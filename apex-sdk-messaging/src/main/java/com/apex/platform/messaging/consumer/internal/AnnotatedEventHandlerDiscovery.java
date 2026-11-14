
package com.apex.platform.messaging.consumer.internal;

import com.apex.platform.messaging.consumer.EventHandler;
import com.apex.platform.messaging.consumer.OnCellEvent;

import org.springframework.aop.support.AopUtils;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public final class AnnotatedEventHandlerDiscovery {

    private final ConfigurableListableBeanFactory beanFactory;

    public AnnotatedEventHandlerDiscovery(
            ConfigurableListableBeanFactory beanFactory
    ) {
        this.beanFactory = beanFactory;
    }

    public List<EventHandler<?>> discover() {

        List<EventHandler<?>> discovered = new ArrayList<>();

        List<String> beanNames = new ArrayList<>();
        beanFactory.getBeanNamesIterator().forEachRemaining(beanNames::add);
        beanNames.sort(String::compareTo);
        for (String beanName : beanNames) {
            Object bean = beanFactory.getSingleton(beanName);
            if (bean instanceof FactoryBean<?>) {
                bean = null; // The singleton here is the factory, not its product.
            }
            Class<?> beanType = bean == null ? beanFactory.getType(beanName, false) : AopProxyUtils.ultimateTargetClass(bean);
            Map<Method, OnCellEvent> methods = methods(beanType);
            if (methods.isEmpty() && bean == null && (beanType == null || beanType == Object.class || beanType.isInterface())
                    && beanFactory.containsBeanDefinition(beanName)
                    && beanFactory.getBeanDefinition(beanName).getRole() == BeanDefinition.ROLE_APPLICATION
                    && beanFactory.isSingleton(beanName)) {
                bean = beanFactory.getBean(beanName);
                methods = methods(AopProxyUtils.ultimateTargetClass(bean));
            }
            if (methods.isEmpty()) {
                continue;
            }
            if (!beanFactory.isSingleton(beanName)) {
                throw new IllegalStateException("@OnCellEvent handler must be a singleton: bean '" + beanName + "'");
            }
            if (bean == null) {
                bean = beanFactory.getBean(beanName);
                methods = methods(AopProxyUtils.ultimateTargetClass(bean));
            }
            List<Method> orderedMethods = new ArrayList<>(methods.keySet());
            orderedMethods.sort(Comparator.comparing(Method::toGenericString));
            for (Method method : orderedMethods) {
                try {
                    if (method.getParameterCount() == 0) {
                        throw new IllegalArgumentException("Handler must have a payload parameter");
                    }
                    Method invocable;
                    try {
                        invocable = AopUtils.selectInvocableMethod(method, bean.getClass());
                    } catch (IllegalStateException failure) {
                        throw new IllegalArgumentException("Handler method is not invocable through the Spring proxy", failure);
                    }
                    OnCellEvent annotation = methods.get(method);
                    discovered.add(new ReflectiveEventHandlerAdapter<>(bean, invocable, annotation.type(),
                            annotation.version(), method.getParameterTypes()[0], beanName));
                } catch (RuntimeException failure) {
                    throw new IllegalStateException("Invalid @OnCellEvent bean '" + beanName + "' method "
                            + method.toGenericString() + ": " + failure.getMessage(), failure);
                }
            }
        }

        return List.copyOf(discovered);
    }

    private static Map<Method, OnCellEvent> methods(Class<?> type) {
        if (type == null) {
            return Map.of();
        }
        return MethodIntrospector.selectMethods(ClassUtils.getUserClass(type),
                (MethodIntrospector.MetadataLookup<OnCellEvent>) method ->
                        AnnotatedElementUtils.findMergedAnnotation(method, OnCellEvent.class));
    }
}
