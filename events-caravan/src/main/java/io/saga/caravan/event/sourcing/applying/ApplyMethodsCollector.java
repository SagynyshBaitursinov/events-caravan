package io.saga.caravan.event.sourcing.applying;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.sourcing.EventSourcedEntity;
import io.saga.caravan.event.sourcing.EventSourcedEntitySetupException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ApplyMethodsCollector {

  private static final ClassValue<Map<String, Method>> APPLY_METHODS_BY_EVENT_NAME = new ClassValue<>() {
    @Override
    protected Map<String, Method> computeValue(Class<?> type) {
      return ApplyMethodsCollector.collectApplyEventMethods(
          type.asSubclass(EventSourcedEntity.class));
    }
  };

  public static Map<String, Method> applyEventMethodsOf(Class<? extends EventSourcedEntity> entityClass) {
    return APPLY_METHODS_BY_EVENT_NAME.get(entityClass);
  }

  private static Map<String, Method> collectApplyEventMethods(Class<? extends EventSourcedEntity> entityClass) {
    Map<String, Method> result = collectFromEntityClassHierarchy(entityClass);

    Map<String, Method> fromEventAppliers = collectFromEventAppliers(entityClass);

    for (Map.Entry<String, Method> entry : fromEventAppliers.entrySet()) {
      if (result.containsKey(entry.getKey())) {
        throw new EventSourcedEntitySetupException(
            "@ApplyEvent method for eventName=%s is duplicated between entity class %s and its @EventApplier classes"
                .formatted(entry.getKey(), entityClass.getName()));
      }
      result.put(entry.getKey(), entry.getValue());
    }

    return Collections.unmodifiableMap(result);
  }

  private static Map<String, Method> collectFromEventAppliers(Class<? extends EventSourcedEntity> entityClass) {
    Map<String, Method> result = new HashMap<>();

    for (Class<?> sourceClass : eventAppliers(entityClass)) {
      Map<String, Method> fromEventApplier = collectFromEventApplierClass(sourceClass, entityClass);

      for (Map.Entry<String, Method> entry : fromEventApplier.entrySet()) {
        if (result.containsKey(entry.getKey())) {
          throw new EventSourcedEntitySetupException(
              "@ApplyEvent method for eventName=%s is duplicated between @EventApplier classes of entity class %s"
                  .formatted(entry.getKey(), entityClass.getName()));
        }
        result.put(entry.getKey(), entry.getValue());
      }
    }

    return result;
  }

  private static Map<String, Method> collectFromEntityClassHierarchy(Class<? extends EventSourcedEntity> entityClass) {
    Map<String, Method> result = new HashMap<>();

    Class<?> classInHierarchy = entityClass;
    while (classInHierarchy != null) {
      Map<String, Method> methodsInClassHierarchy = new HashMap<>();

      for (Method method : classInHierarchy.getDeclaredMethods()) {
        var applyEventAnnotation = method.getAnnotation(ApplyEvent.class);

        if (applyEventAnnotation == null) {
          continue;
        }

        var eventName = applyEventAnnotation.value();

        validateApplyMethodSignature(method, classInHierarchy);

        if (methodsInClassHierarchy.containsKey(eventName)) {
          throw new EventSourcedEntitySetupException(
              "@ApplyEvent method for eventName=%s is duplicated in entity class %s"
                  .formatted(eventName, classInHierarchy.getName()));
        }
        methodsInClassHierarchy.put(eventName, method);
        method.setAccessible(true);
      }

      methodsInClassHierarchy.forEach(result::putIfAbsent);
      classInHierarchy = classInHierarchy.getSuperclass();
    }

    return result;
  }

  private static Set<Class<?>> eventAppliers(Class<? extends EventSourcedEntity> entityClass) {
    Set<Class<?>> eventApplierClasses = new HashSet<>();

    var eventApplierAnnotation = entityClass.getDeclaredAnnotation(EventApplier.class);

    if (eventApplierAnnotation != null) {
      eventApplierClasses.addAll(List.of(eventApplierAnnotation.value()));
    }

    return eventApplierClasses;
  }

  private static Map<String, Method> collectFromEventApplierClass(Class<?> eventApplierClass,
                                                                  Class<? extends EventSourcedEntity> entityClass) {
    Map<String, Method> result = new HashMap<>();

    for (Method method : eventApplierClass.getDeclaredMethods()) {
      var applyEventAnnotation = method.getAnnotation(ApplyEvent.class);

      if (applyEventAnnotation == null) {
        continue;
      }

      var eventName = applyEventAnnotation.value();

      validateApplyMethodSignature(method, eventApplierClass, entityClass);

      if (result.containsKey(eventName)) {
        throw new EventSourcedEntitySetupException(
            "@ApplyEvent method for eventName=%s is duplicated in @EventApplier class %s"
                .formatted(eventName, eventApplierClass.getName()));
      }
      result.put(eventName, method);
      method.setAccessible(true);
    }

    return result;
  }

  private static void validateApplyMethodSignature(Method method,
                                                   Class<?> classInHierarchy) {
    if (method.getParameters().length != 1
        || !(method.getGenericParameterTypes()[0] instanceof ParameterizedType parametrizedType)
        || !parametrizedType.getRawType().equals(Event.class)) {
      throw new EventSourcedEntitySetupException(
          "@ApplyEvent method must have only one Event<PayloadClass> parameter, which is not the case for %s.%s"
              .formatted(classInHierarchy.getName(), method.getName()));
    }

    validateConcretePayload(parametrizedType, method, classInHierarchy);
  }

  private static void validateApplyMethodSignature(Method method,
                                                   Class<?> eventApplierClass,
                                                   Class<? extends EventSourcedEntity> entityClass) {
    if (!Modifier.isStatic(method.getModifiers())) {
      throw new EventSourcedEntitySetupException(
          "@ApplyEvent method declared in  @EventApplier must be static, which is not the case for %s.%s"
              .formatted(eventApplierClass.getName(), method.getName()));
    }

    if (method.getParameters().length != 2
        || method.getParameterTypes()[0] != entityClass
        || !(method.getGenericParameterTypes()[1] instanceof ParameterizedType parametrizedType)
        || !parametrizedType.getRawType().equals(Event.class)) {
      throw new EventSourcedEntitySetupException(
          "@ApplyEvent method declared in @EventApplier must have (%s, Event<PayloadClass>) parameters, which is not the case for %s.%s"
              .formatted(entityClass.getSimpleName(), eventApplierClass.getName(), method.getName()));
    }

    validateConcretePayload(parametrizedType, method, eventApplierClass);
  }

  private static void validateConcretePayload(ParameterizedType eventParameterType,
                                              Method method,
                                              Class<?> declaringClass) {
    Type payloadTypeArg = eventParameterType.getActualTypeArguments()[0];
    if (!(payloadTypeArg instanceof Class<?>)) {
      throw new EventSourcedEntitySetupException(
          "@ApplyEvent Event parameter's payload class must be a concrete class, which is not the case for %s.%s"
              .formatted(declaringClass.getName(), method.getName()));
    }
  }
}
