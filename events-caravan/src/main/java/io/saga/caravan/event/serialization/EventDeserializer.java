package io.saga.caravan.event.serialization;

import io.saga.caravan.event.Event;

public interface EventDeserializer {

  Event<?> deserialize(String eventAsJson) throws EventDeserializationException;
}
