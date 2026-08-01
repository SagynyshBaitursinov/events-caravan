package io.saga.caravan.event.producer;

import io.saga.caravan.event.EntityEventsRegistry;
import io.saga.caravan.event.Event;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
public class ValidatingEventProducer implements EventProducer {

  private final EventProducer delegate;
  private final EntityEventsRegistry entityEventsRegistry;

  @Override
  public void produce(Event<?> event) {
    validate(event);
    delegate.produce(event);
  }

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
  }
}
