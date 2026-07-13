package io.saga.caravan.event.producer;

import io.saga.caravan.event.Event;

import java.util.Collection;

public interface EventProducer {

  void produce(Event<?> event);

  void produce(Collection<Event<?>> events);
}
