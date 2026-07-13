package io.saga.caravan.event.sourcing.applying;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.EventType;
import io.saga.caravan.event.sourcing.EventSourcedEntity;
import io.saga.caravan.event.sourcing.EventSourcedEntitySetupException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class ApplyMethodsCollector {

  private final Map<EventType, Class<?>> eventPayloadClassMap;

  public Map<EventType, Method> collectApplyEventMethods(Class<? extends EventSourcedEntity> targetClass,
                                                         String entityName) {
    Map<EventType, Method> result = new HashMap<>();

    Class<?> classInHierarchy = targetClass;
    while (classInHierarchy != null) {
      Map<EventType, Method> methodsInClassHierarchy = new HashMap<>();

      for (Method method : classInHierarchy.getDeclaredMethods()) {
        var applyEventAnnotation = method.getAnnotation(ApplyEvent.class);

        if (applyEventAnnotation == null) {
          continue;
        }

        var eventType = new EventType(entityName, applyEventAnnotation.value());

        validateEventPayloadClass(method, classInHierarchy, eventType);

        if (methodsInClassHierarchy.containsKey(eventType)) {
          throw new EventSourcedEntitySetupException(
              "@ApplyEvent method for %s is duplicated".formatted(eventType));
        }
        methodsInClassHierarchy.put(eventType, method);
        method.setAccessible(true);
      }

      methodsInClassHierarchy.forEach(result::putIfAbsent);
      classInHierarchy = classInHierarchy.getSuperclass();
    }

    return result;
  }

  @SuppressWarnings("ExtractMethodRecommender")
  private void validateEventPayloadClass(Method method,
                                         Class<?> classInHierarchy,
                                         EventType eventType) {
    if (method.getParameters().length != 1
        || !(method.getGenericParameterTypes()[0] instanceof ParameterizedType parametrizedType)
        || !parametrizedType.getRawType().equals(Event.class)) {
      throw new EventSourcedEntitySetupException(
          "@ApplyEvent method must have only one Event<PayloadClass> parameter, which is not the case for %s.%s"
              .formatted(classInHierarchy.getName(), method.getName()));
    }

    Type payloadTypeArg = parametrizedType.getActualTypeArguments()[0];
    if (!(payloadTypeArg instanceof Class<?> payloadClass)) {
      throw new EventSourcedEntitySetupException(
          "@ApplyEvent Event parameter's payload class must be a concrete class, which is not the case for %s.%s"
              .formatted(classInHierarchy.getName(), method.getName()));
    }

    if (!payloadClass.equals(eventPayloadClassMap.get(eventType))) {
      throw new EventSourcedEntitySetupException(
          "@ApplyEvent Event parameter's payload class must be the one from EventPayloadRegistration, which is not the case for %s.%s"
              .formatted(classInHierarchy.getName(), method.getName()));
    }
  }
}
