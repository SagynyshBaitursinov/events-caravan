package io.saga.caravan.event.sourcing;

import io.saga.caravan.entity.Entity;
import io.saga.caravan.event.Event;
import lombok.extern.slf4j.Slf4j;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public abstract class EventSourcedEntity extends Entity {

  private final List<Event<?>> notProducedEvents = new ArrayList<>();

  private long version = 0L;

  public final long version() {
    return version;
  }

  final void setVersion(long version) {
    this.version = version;
  }

  protected final <E> void recordEvent(String eventName,
                                       E eventPayload) {
    var event = buildEvent(eventName, eventPayload);
    log.debug("Recording {}", event.eventReference());

    notProducedEvents.add(event);

    EntityEventApplier.apply(this, event);
  }

  private <E> Event<E> buildEvent(String eventName,
                                  E eventPayload) {
    return Event.<E>builder()
        .entityReference(this.entityReference())
        .sequenceNumber(this.version + 1)
        .eventName(eventName)
        .timestamp(ZonedDateTime.now())
        .payload(eventPayload)
        .build();
  }

  final List<Event<?>> notProducedEvents() {
    return Collections.unmodifiableList(notProducedEvents);
  }

  final void clearNotProducedEvents() {
    notProducedEvents.clear();
  }

  final boolean hasBlankState() {
    return version == 0L;
  }
}