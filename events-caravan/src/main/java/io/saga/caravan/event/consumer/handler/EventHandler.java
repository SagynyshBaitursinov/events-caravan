package io.saga.caravan.event.consumer.handler;

import io.saga.caravan.event.Event;

/**
 * Handles events whose payload is of type {@code T}. Implemented by applications and registered
 * with a {@link HandlerBasedEventConsumer}, which dispatches each event to every handler whose
 * {@code T} matches the event's payload type and that reports interest via
 * {@link #isOfInterest(Event)}.
 *
 * @param <T> the payload type this handler is interested in
 */
public interface EventHandler<T> {

  /**
   * Whether this handler wants to process the given event. Called only after the event's
   * payload type has already been matched against {@code T}.
   */
  boolean isOfInterest(Event<T> event);

  /**
   * Processes the given event. Called only when {@link #isOfInterest(Event)} returned {@code true}.
   */
  void handle(Event<T> event);
}
