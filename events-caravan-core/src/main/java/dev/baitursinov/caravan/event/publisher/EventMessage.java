package dev.baitursinov.caravan.event.publisher;

import dev.baitursinov.caravan.event.EventReference;

import static java.util.Objects.requireNonNull;

/**
 * A serialized event ready to be handed to a message broker: its {@code body}, as produced by an
 * {@link dev.baitursinov.caravan.event.serialization.EventSerializer}, accompanied by the
 * {@link EventReference} it was serialized from.
 */
public record EventMessage(EventReference eventReference,
                           String body) {

  public EventMessage {
    requireNonNull(eventReference, "eventReference cannot be null");
    requireNonNull(body, "body cannot be null");
  }

  @Override
  public String toString() {
    return "EventMessage{" + eventReference + "}";
  }
}
