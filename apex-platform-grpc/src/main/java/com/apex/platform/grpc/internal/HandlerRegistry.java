package com.apex.platform.grpc.internal;

import com.apex.platform.grpc.api.*;
import java.lang.reflect.*;
import java.util.*;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;

public final class HandlerRegistry implements SmartInitializingSingleton {
  record Key(boolean query, String type, int version) {}

  public record Handler(
      Object bean, Method method, Contracts.Contract contract, boolean contextParameter) {
    Object invoke(Object payload, RequestContext context) throws Throwable {
      try {
        return contextParameter
            ? method.invoke(bean, payload, context)
            : method.invoke(bean, payload);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }

  private final ConfigurableListableBeanFactory factory;
  private final Map<Key, Handler> handlers = new HashMap<>();

  public HandlerRegistry(ConfigurableListableBeanFactory factory) {
    this.factory = factory;
  }

  @Override
  public void afterSingletonsInstantiated() {
    var names = new ArrayList<String>();
    factory.getBeanNamesIterator().forEachRemaining(names::add);
    Collections.sort(names);
    for (String name : names) {
      Object bean = factory.getSingleton(name);
      if (bean instanceof org.springframework.beans.factory.FactoryBean<?>) bean = null;
      Class<?> type =
          bean == null ? factory.getType(name, false) : AopProxyUtils.ultimateTargetClass(bean);
      if (type == null) continue;
      var methods =
          MethodIntrospector.selectMethods(
              type,
              (MethodIntrospector.MetadataLookup<Boolean>)
                  m ->
                      AnnotatedElementUtils.hasAnnotation(m, CellRequestHandler.class)
                              || AnnotatedElementUtils.hasAnnotation(m, CellQueryHandler.class)
                          ? Boolean.TRUE
                          : null);
      if (methods.isEmpty()) continue;
      if (!factory.isSingleton(name))
        throw new IllegalStateException("RPC handler must be singleton: " + name);
      if (bean == null) bean = factory.getBean(name);
      for (Method method : methods.keySet()) {
        try {
          boolean query = AnnotatedElementUtils.hasAnnotation(method, CellQueryHandler.class);
          var request =
              AnnotatedElementUtils.findMergedAnnotation(method, CellRequestHandler.class);
          var annotation =
              AnnotatedElementUtils.findMergedAnnotation(method, CellQueryHandler.class);
          if (query && request != null)
            throw new IllegalArgumentException("One handler kind required");
          int count = method.getParameterCount();
          if (!Modifier.isPublic(method.getModifiers())
              || Modifier.isStatic(method.getModifiers())
              || (AopUtils.isCglibProxy(bean) && Modifier.isFinal(method.getModifiers()))
              || count < 1
              || count > 2
              || (count == 2 && method.getParameterTypes()[1] != RequestContext.class))
            throw new IllegalArgumentException(
                "Public invocable payload + optional RequestContext required");
          var c = Contracts.inspect(method.getParameterTypes()[0], query);
          String logical = query ? annotation.type() : request.type();
          int version = query ? annotation.version() : request.version();
          if (!c.type().equals(logical)
              || c.version() != version
              || method.getReturnType() != c.response())
            throw new IllegalArgumentException("Handler contract/response mismatch");
          Method invocable = AopUtils.selectInvocableMethod(method, bean.getClass());
          if (!invocable.canAccess(bean) && !invocable.trySetAccessible())
            throw new IllegalArgumentException("Handler inaccessible");
          if (handlers.putIfAbsent(
                  new Key(query, logical, version), new Handler(bean, invocable, c, count == 2))
              != null)
            throw new IllegalArgumentException("Duplicate RPC handler " + logical + " V" + version);
        } catch (RuntimeException failure) {
          throw new IllegalStateException(
              "Invalid RPC bean '"
                  + name
                  + "' method "
                  + method.toGenericString()
                  + ": "
                  + failure.getMessage(),
              failure);
        }
      }
    }
  }

  public Handler find(boolean query, String type, int version) {
    var h = handlers.get(new Key(query, type, version));
    if (h == null) throw new GatewayException(GatewayException.Code.UNIMPLEMENTED, false);
    return h;
  }

  public boolean hasRequests() {
    return handlers.keySet().stream().anyMatch(k -> !k.query());
  }

  public int size() {
    return handlers.size();
  }
}
