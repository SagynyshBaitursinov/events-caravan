package io.saga.caravan.event.producer;

import io.saga.caravan.event.Event;

import java.util.List;

/**
 * Shall be used wrapped with {@link ValidatingEventProducer} to validate Events
 * against an {@link io.saga.caravan.event.EntityEventsRegistry} before they are produced.
 */
public interface EventProducer {

  void produce(Event<?> event);

  void produce(List<Event<?>> events);
}
