package dev.baitursinov.caravan.event.serialization;

import dev.baitursinov.caravan.event.Event;

/**
 * Deserializes a whole {@link Event}, including its metadata, from the wire format produced by
 * the matching {@link EventSerializer}.
 */
public interface EventDeserializer {

  Event<?> deserialize(String eventMessage) throws EventDeserializationException;
}
