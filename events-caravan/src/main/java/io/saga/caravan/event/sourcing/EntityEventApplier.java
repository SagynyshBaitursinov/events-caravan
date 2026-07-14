package io.saga.caravan.event.sourcing;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.sourcing.applying.ApplyMethodsCollector;
import io.saga.caravan.event.sourcing.applying.EventApplyingException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public final class EntityEventApplier {

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

  public static void apply(EventSourcedEntity entity,
                           Event<?> event) {
    var applyMethod =
        applyEventMethodsOf(entity.getClass()).get(event.eventName());

    if (applyMethod == null) {
      throw new EventApplyingException(
          "No @ApplyEvent method for eventName=%s in %s"
              .formatted(event.eventName(), entity.getClass().getName()));
    }

    try {
      applyMethod.invoke(entity, event);
      incrementEntityVersion(entity, event);
    } catch (IllegalArgumentException | IllegalAccessException illegalAccessException) {
      throw new EventApplyingException(
          "Cannot invoke apply method %s.%s with parameter %s, check its setup according to EventPayloadRegistration"
              .formatted(
                  entity.getClass().getName(),
                  applyMethod.getName(),
                  event),
          illegalAccessException);
    } catch (InvocationTargetException invocationTargetException) {
      throw new EventApplyingException(invocationTargetException.getTargetException());
    }
  }

  private static void incrementEntityVersion(EventSourcedEntity entity,
                                             Event<?> event) {
    try {
      entity.setVersion(event.sequenceNumber());
    } catch (NumberFormatException exception) {
      throw new EventApplyingException(
          "%s was expected to contains a sequence number for event sourcing"
              .formatted(event),
          exception);
    }
  }
}
