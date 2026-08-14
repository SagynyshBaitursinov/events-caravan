package dev.baitursinov.caravan.event.sourcing;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.sourcing.applying.ApplyMethodsCollector;
import dev.baitursinov.caravan.event.sourcing.applying.EventApplyingException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EntityEventApplier {

  public static void apply(EventSourcedEntity entity,
                           Event<?> event) {
    var applyMethod =
        ApplyMethodsCollector
            .applyEventMethodsOf(entity.getClass())
            .get(event.eventName());

    if (applyMethod == null) {
      throw new EventApplyingException(
          "No @ApplyEvent method for eventName=%s in entity class %s"
              .formatted(event.eventName(), entity.getClass().getName()));
    }

    invoke(entity, event, applyMethod);

    entity.setVersion(event.sequenceNumber());
  }

  private static void invoke(EventSourcedEntity entity,
                             Event<?> event,
                             Method applyMethod) {
    try {
      if (Modifier.isStatic(applyMethod.getModifiers())) {
        applyMethod.invoke(null, entity, event);
      } else {
        applyMethod.invoke(entity, event);
      }
    } catch (IllegalArgumentException | IllegalAccessException illegalAccessException) {
      throw new EventApplyingException(
          getCannotInvokeMessage(entity, event, applyMethod),
          illegalAccessException);
    } catch (InvocationTargetException invocationTargetException) {
      throw new EventApplyingException(
          getCannotInvokeMessage(entity, event, applyMethod),
          invocationTargetException.getTargetException());
    }
  }

  private static String getCannotInvokeMessage(EventSourcedEntity entity,
                                               Event<?> event,
                                               Method applyMethod) {
    return "Cannot invoke apply method %s.%s with parameter %s of payload type %s"
        .formatted(
            entity.getClass().getName(),
            applyMethod.getName(),
            event,
            event.payload().getClass());
  }
}
