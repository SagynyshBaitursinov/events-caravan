package dev.baitursinov.caravan.event.consumer;

import dev.baitursinov.caravan.event.Event;

/**
 * Consumes events on the receiving side of an application's event flow. Implemented by
 * applications that want to react to events, either directly or via
 * {@link dev.baitursinov.caravan.event.consumer.handler.HandlerBasedEventConsumer}.
 */
public interface EventConsumer {

  void consume(Event<?> event);
}
