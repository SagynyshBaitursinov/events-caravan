package io.saga.caravan.event.consumer.handler;

import io.saga.caravan.event.Event;

public interface EventHandler<T> {

  boolean isOfInterest(Event<T> event);

  void handle(Event<T> event);
}
