package dev.baitursinov.caravan.event.producer;

import dev.baitursinov.caravan.event.EntityEventsRegistry;
import dev.baitursinov.caravan.event.Event;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Validates an event's payload class against the {@link EntityEventsRegistry}, to catch
 * mismatches between an event's declared type and its actual payload.
 */
@Slf4j
@RequiredArgsConstructor
final class EventPayloadClassValidator {

  private final EntityEventsRegistry entityEventsRegistry;

  /**
   * @throws EventProductionException if the event's type has no registered payload class, or
   *                                  its payload's class does not match the registered one
   */
  void validate(Event<?> event) {
    var registeredPayloadClass = entityEventsRegistry.payloadClassFor(event.eventType())
        .orElseThrow(() -> new EventProductionException(
            "Cannot produce %s, because its %s has no registered payload class"
                .formatted(event.eventReference(), event.eventType())));

    if (!registeredPayloadClass.equals(event.payload().getClass())) {
      throw new EventProductionException(
          "Cannot produce %s, because its payload class %s does not match %s registered for %s"
              .formatted(
                  event.eventReference(),
                  event.payload().getClass(),
                  registeredPayloadClass,
                  event.eventType()));
    }

    log.debug("Successfully validated {} against registered payload class {}",
        event.eventReference(), registeredPayloadClass);
  }
}
