package io.saga.caravan.event.producer;

import io.saga.caravan.event.EntityEventsRegistry;
import io.saga.caravan.event.Event;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * An {@link EventProducer} decorator that validates each event's payload class against the
 * {@link EntityEventsRegistry} before delegating production to another {@link EventProducer}.
 * Applications wrap their transport-specific producer with this class to catch mismatches
 * between an event's declared type and its actual payload before it is published.
 */
@Slf4j
@RequiredArgsConstructor
public class ValidatingEventProducer implements EventProducer {

  private final EventProducer delegate;
  private final EntityEventsRegistry entityEventsRegistry;

  /**
   * @throws EventProductionException if the event's type has no registered payload class, or
   *                                  its payload's class does not match the registered one
   */
  @Override
  public void produce(Event<?> event) {
    validate(event);
    delegate.produce(event);
  }

  /**
   * @throws EventProductionException if any event's type has no registered payload class, or
   *                                  its payload's class does not match the registered one
   */
  @Override
  public void produce(List<Event<?>> events) {
    events.forEach(this::validate);
    delegate.produce(events);
  }

  private void validate(Event<?> event) {
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
