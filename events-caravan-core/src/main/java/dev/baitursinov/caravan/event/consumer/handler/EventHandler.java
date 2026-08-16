package dev.baitursinov.caravan.event.consumer.handler;

import dev.baitursinov.caravan.event.Event;

/**
 * Handles events whose payload is of type {@code T}. Implemented by applications and registered
 * with a {@link HandlerBasedEventConsumer}, which dispatches each event to every handler whose
 * {@code T} matches the event's payload type and that reports interest via
 * {@link #isOfInterest(Event)}.
 * <p>
 * It's important that EventHandlers are idempotent and tolerant of out-of-order arrival.
 * sequenceNumber of Events per EntityReference are gapless and in increasing order. However, real idempotence and ordering must be
 * ensured by the application behavior.
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
