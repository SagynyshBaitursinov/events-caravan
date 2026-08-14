package dev.baitursinov.caravan.event.serialization;

import dev.baitursinov.caravan.event.EventType;

/**
 * Deserializes only an event's payload from the wire format produced by a matching
 * {@link EventPayloadSerializer}, resolving the target payload class from the event's
 * {@link EventType}.
 */
public interface EventPayloadDeserializer {

  Object deserializePayload(String payload,
                            EventType eventType) throws EventPayloadDeserializationException;
}
