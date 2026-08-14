package dev.baitursinov.caravan.event.serialization;

import dev.baitursinov.caravan.event.Event;

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
