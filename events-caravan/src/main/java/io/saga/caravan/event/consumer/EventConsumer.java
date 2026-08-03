package io.saga.caravan.event.consumer;

import io.saga.caravan.event.Event;

/**
 * Consumes events on the receiving side of an application's event flow. Implemented by
 * applications that want to react to events, either directly or via
 * {@link io.saga.caravan.event.consumer.handler.HandlerBasedEventConsumer}.
 */
public interface EventConsumer {

  void consume(Event<?> event);
}
