package io.saga.caravan.event.consumer;

import io.saga.caravan.event.Event;

public interface EventConsumer {

  void consume(Event<?> event);
}
