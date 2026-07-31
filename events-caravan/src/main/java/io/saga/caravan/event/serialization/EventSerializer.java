package io.saga.caravan.event.serialization;

import io.saga.caravan.event.Event;

public interface EventSerializer {

  String serialize(Event<?> event) throws EventSerializationException;
}
