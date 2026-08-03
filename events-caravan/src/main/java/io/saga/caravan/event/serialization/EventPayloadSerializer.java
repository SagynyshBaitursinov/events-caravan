package io.saga.caravan.event.serialization;

import io.saga.caravan.event.Event;

/**
 * Serializes only an event's payload leaving its metadata
 * (entity reference, sequence number, event name, timestamp) to be handled separately by an
 * {@link EventSerializer}.
 * <p>
 * Implement this to control how payload classes are serialized.
 */
public interface EventPayloadSerializer {

  String serializePayload(Event<?> event);
}
