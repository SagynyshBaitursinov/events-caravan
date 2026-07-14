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
import java.util.LinkedHashSet;
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

  private static Map<String, Method> collectApplyEventMethods(Class<? extends EventSourcedEntity> targetClass) {
    Map<String, Method> result = collectFromClassHierarchy(targetClass);

    for (Class<?> sourceClass : applyEventSourcesOf(targetClass)) {
      collectFromSourceClass(sourceClass, targetClass, result);
    }

    return Collections.unmodifiableMap(result);
  }

  private static Map<String, Method> collectFromClassHierarchy(Class<? extends EventSourcedEntity> targetClass) {
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

        validateEntityMethodSignature(method, classInHierarchy);

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

    return result;
  }

  private static Set<Class<?>> applyEventSourcesOf(Class<? extends EventSourcedEntity> targetClass) {
    Set<Class<?>> sourceClasses = new LinkedHashSet<>();

    Class<?> classInHierarchy = targetClass;
    while (classInHierarchy != null) {
      var applyEventSourcesAnnotation = classInHierarchy.getDeclaredAnnotation(ApplyEventSources.class);

      if (applyEventSourcesAnnotation != null) {
        sourceClasses.addAll(List.of(applyEventSourcesAnnotation.value()));
      }
      classInHierarchy = classInHierarchy.getSuperclass();
    }

    return sourceClasses;
  }

  private static void collectFromSourceClass(Class<?> sourceClass,
                                             Class<? extends EventSourcedEntity> targetClass,
                                             Map<String, Method> result) {
    for (Method method : sourceClass.getDeclaredMethods()) {
      var applyEventAnnotation = method.getAnnotation(ApplyEvent.class);

      if (applyEventAnnotation == null) {
        continue;
      }

      validateSourceMethodSignature(method, sourceClass, targetClass);

      var eventName = applyEventAnnotation.value();

      var alreadyCollectedMethod = result.get(eventName);
      if (alreadyCollectedMethod != null) {
        throw new EventSourcedEntitySetupException(
            "@ApplyEvent method for eventName=%s is declared both in %s.%s and %s.%s"
                .formatted(
                    eventName,
                    alreadyCollectedMethod.getDeclaringClass().getName(),
                    alreadyCollectedMethod.getName(),
                    method.getDeclaringClass().getName(),
                    method.getName()));
      }
      result.put(eventName, method);
      method.setAccessible(true);
    }
  }

  private static void validateEntityMethodSignature(Method method,
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

  private static void validateSourceMethodSignature(Method method,
                                                    Class<?> sourceClass,
                                                    Class<? extends EventSourcedEntity> targetClass) {
    if (!Modifier.isStatic(method.getModifiers())) {
      throw new EventSourcedEntitySetupException(
          "@ApplyEvent method declared outside of an entity class must be static, which is not the case for %s.%s"
              .formatted(sourceClass.getName(), method.getName()));
    }

    if (method.getParameters().length != 2
        || !EventSourcedEntity.class.isAssignableFrom(method.getParameterTypes()[0])
        || !(method.getGenericParameterTypes()[1] instanceof ParameterizedType parametrizedType)
        || !parametrizedType.getRawType().equals(Event.class)) {
      throw new EventSourcedEntitySetupException(
          "@ApplyEvent method declared outside of an entity class must have (EntityClass, Event<PayloadClass>) parameters, which is not the case for %s.%s"
              .formatted(sourceClass.getName(), method.getName()));
    }

    if (!method.getParameterTypes()[0].isAssignableFrom(targetClass)) {
      throw new EventSourcedEntitySetupException(
          "@ApplyEvent method's first parameter must be assignable from %s declaring it in @ApplyEventSources, which is not the case for %s.%s"
              .formatted(targetClass.getName(), sourceClass.getName(), method.getName()));
    }

    validateConcretePayload(parametrizedType, method, sourceClass);
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
