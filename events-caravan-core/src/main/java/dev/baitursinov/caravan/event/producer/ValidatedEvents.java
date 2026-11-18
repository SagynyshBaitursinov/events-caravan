package dev.baitursinov.caravan.event.producer;

import dev.baitursinov.caravan.event.EntityEventsRegistry;
import dev.baitursinov.caravan.event.Event;

import java.util.List;

/**
 * A batch of events that a producer has validated against the {@link EntityEventsRegistry}. It is
 * what {@link SequencedEventWriter}s and {@link UnsequencedEventWriter}s receive, and only
 * {@link SequencedEventProducer} and {@link UnsequencedEventProducer} can create one, so events
 * cannot reach a writer without having been validated.
 */
public final class ValidatedEvents {

  private final List<Event<?>> events;

  ValidatedEvents(List<Event<?>> events) {
    this.events = List.copyOf(events);
  }

  /**
   * The validated events, in the order they were given to the producer.
   */
  public List<Event<?>> events() {
    return events;
  }

  public boolean isEmpty() {
    return events.isEmpty();
  }

  public int size() {
    return events.size();
  }

  @Override
  public String toString() {
    return "ValidatedEvents" + events;
  }
}
