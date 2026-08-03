package io.saga.caravan.event.serialization;

import io.saga.caravan.event.Event;

/**
 * Serializes a whole {@link Event}, including its metadata.
 * <p>
 * Implement this to use a different format.
 */
public interface EventSerializer {

  String serialize(Event<?> event) throws EventSerializationException;
}
