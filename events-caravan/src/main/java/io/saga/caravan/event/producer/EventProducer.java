package io.saga.caravan.event.producer;

import io.saga.caravan.event.Event;

import java.util.List;

/**
 * Publishes events to their destination. Implemented by
 * extenders for a specific transport; applications call {@link #produce(Event)} or
 * {@link #produce(List)} to produce event.
 * <p>
 * Implementations must ensure that events being published get published atomically,
 * result is durable, eventual propagation to all consumers happens,
 * duplicate event production is detected and fails synchronously.
 *
 * <p>Should be used wrapped with {@link ValidatingEventProducer} to validate Events
 * against an {@link io.saga.caravan.event.EntityEventsRegistry} before they are produced.
 */
public interface EventProducer {

  /**
   * Produces a single event.
   *
   * @throws EventProductionException          if the event cannot be produced
   * @throws DuplicateEventProductionException if event has already been produced for the
   *                                           same entity and sequence number, signaling a concurrent modification of that entity.
   */
  void produce(Event<?> event);

  /**
   * Produces a batch of events, typically all belonging to the same entity.
   *
   * @throws EventProductionException          if the event cannot be produced
   * @throws DuplicateEventProductionException if event has already been produced for the
   *                                           same entity and sequence number, signaling a concurrent modification of that entity.
   */
  void produce(List<Event<?>> events);
}
