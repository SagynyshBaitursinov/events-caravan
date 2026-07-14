package io.saga.caravan.event.sourcing.applying;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.sourcing.EventSourcedEntity;
import io.saga.caravan.event.sourcing.EventSourcedEntitySetupException;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class ApplyMethodsCollector {

  public static Map<String, Method> collectApplyEventMethods(Class<? extends EventSourcedEntity> targetClass) {
    Map<String, Method> result = new HashMap<>();

    Class<?> classInHierarchy = targetClass;
    while (classInHierarchy != null) {
      Map<String, Method> methodsInClassHierarchy = new HashMap<>();

      for (Method method : classInHierarchy.getDeclaredMethods()) {
        var applyEventAnnotation = method.getAnnotation(ApplyEvent.class);

        if (applyEventAnnotation == null) {
          continue;
        }

        var eventName = applyEventAnnotation.value();

        validateMethodSignature(method, classInHierarchy);

        if (methodsInClassHierarchy.containsKey(eventName)) {
          throw new EventSourcedEntitySetupException(
              "@ApplyEvent method for eventName=%s is duplicated in %s"
                  .formatted(eventName, classInHierarchy.getName()));
        }
        methodsInClassHierarchy.put(eventName, method);
        method.setAccessible(true);
      }

      methodsInClassHierarchy.forEach(result::putIfAbsent);
      classInHierarchy = classInHierarchy.getSuperclass();
    }

    return Collections.unmodifiableMap(result);
  }

  private static void validateMethodSignature(Method method,
                                              Class<?> classInHierarchy) {
    if (method.getParameters().length != 1
        || !(method.getGenericParameterTypes()[0] instanceof ParameterizedType parametrizedType)
        || !parametrizedType.getRawType().equals(Event.class)) {
      throw new EventSourcedEntitySetupException(
          "@ApplyEvent method must have only one Event<PayloadClass> parameter, which is not the case for %s.%s"
              .formatted(classInHierarchy.getName(), method.getName()));
    }

    Type payloadTypeArg = parametrizedType.getActualTypeArguments()[0];
    if (!(payloadTypeArg instanceof Class<?>)) {
      throw new EventSourcedEntitySetupException(
          "@ApplyEvent Event parameter's payload class must be a concrete class, which is not the case for %s.%s"
              .formatted(classInHierarchy.getName(), method.getName()));
    }
  }
}
