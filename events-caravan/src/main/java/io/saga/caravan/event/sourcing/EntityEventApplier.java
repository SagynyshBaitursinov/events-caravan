package io.saga.caravan.event.sourcing;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.EventType;
import io.saga.caravan.event.sourcing.applying.EventApplyingException;
import lombok.RequiredArgsConstructor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

@RequiredArgsConstructor
public class EntityEventApplier {

  private final Map<EventType, Method> applyMethodProvider;

  public void apply(EventSourcedEntity entity,
                    Event<?> event) {
    var applyMethod = applyMethodProvider.get(event.eventType());
    if (applyMethod == null) {
      throw new EventApplyingException("Cannot apply event as entityEventApplier is not initialized");
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

  private void incrementEntityVersion(EventSourcedEntity entity,
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
