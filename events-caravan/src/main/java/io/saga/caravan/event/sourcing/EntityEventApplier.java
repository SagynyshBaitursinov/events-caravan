package io.saga.caravan.event.sourcing;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.sourcing.applying.ApplyMethodsCollector;
import io.saga.caravan.event.sourcing.applying.EventApplyingException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EntityEventApplier {

  public static void apply(EventSourcedEntity entity,
                           Event<?> event) {
    var applyMethod =
        ApplyMethodsCollector.applyEventMethodsOf(entity.getClass()).get(event.eventName());

    if (applyMethod == null) {
      throw new EventApplyingException(
          "No @ApplyEvent method for eventName=%s in %s"
              .formatted(event.eventName(), entity.getClass().getName()));
    }

    try {
      if (Modifier.isStatic(applyMethod.getModifiers())) {
        applyMethod.invoke(null, entity, event);
      } else {
        applyMethod.invoke(entity, event);
      }
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
