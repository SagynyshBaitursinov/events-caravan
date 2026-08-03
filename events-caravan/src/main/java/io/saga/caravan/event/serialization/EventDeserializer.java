package io.saga.caravan.event.serialization;

import io.saga.caravan.event.Event;

/**
 * Deserializes a whole {@link Event}, including its metadata, from the wire format produced by
 * the matching {@link EventSerializer}.
 */
public interface EventDeserializer {

  Event<?> deserialize(String eventMessage) throws EventDeserializationException;
}
